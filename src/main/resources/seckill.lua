-- KEYS: transaction hash, stock string, purchaser set, owner hash.
-- ARGV: PREPARE/RESERVE/CHECK, voucherId, userId, orderId, prepareTimeoutMs.
-- 0 RESERVED; 1 REJECTED(stock); 2 REJECTED(duplicate); 3 ABORTED; 4 PREPARED; -1 uncertain.
-- Four business states only. writePending/review are diagnostic guards.
local txn, stockKey, buyers, owners = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local action, voucherId, userId, orderId = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local timeout = tonumber(ARGV[5])
local function id(value) return value and string.match(value, '^[1-9]%d*$') end
local function kind(key) return redis.call('TYPE', key).ok end
if not id(voucherId) or not id(userId) or not id(orderId) or not timeout or timeout <= 0
    or (action ~= 'PREPARE' and action ~= 'RESERVE' and action ~= 'CHECK') then return -1 end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local txnType = kind(txn)
if txnType == 'none' then
    if action ~= 'PREPARE' then return -1 end
    redis.call('HSET', txn, 'orderId', orderId, 'userId', userId, 'voucherId', voucherId,
        'state', 'PREPARED', 'createdAt', now, 'deadline', now + timeout, 'writePending', '0')
    return 4
end
if txnType ~= 'hash' or redis.call('PTTL', txn) ~= -1 then return -1 end
if redis.call('HGET', txn, 'orderId') ~= orderId or redis.call('HGET', txn, 'userId') ~= userId
    or redis.call('HGET', txn, 'voucherId') ~= voucherId then return -1 end
local function uncertain(reason)
    redis.call('HSET', txn, 'review', '1', 'diagnostic', reason)
    return -1
end
if redis.call('HGET', txn, 'writePending') ~= '0' or redis.call('HGET', txn, 'review') == '1' then return -1 end
local state = redis.call('HGET', txn, 'state')
local deadline = tonumber(redis.call('HGET', txn, 'deadline'))
if not deadline or not tonumber(redis.call('HGET', txn, 'createdAt')) then return uncertain('invalid timestamps') end
local ownerType, buyersType = kind(owners), kind(buyers)
if (ownerType ~= 'none' and (ownerType ~= 'hash' or redis.call('PTTL', owners) ~= -1))
    or (buyersType ~= 'none' and (buyersType ~= 'set' or redis.call('PTTL', buyers) ~= -1)) then
    return uncertain('invalid ownership type or TTL')
end
local owner = redis.call('HGET', owners, userId)
local member = redis.call('SISMEMBER', buyers, userId)
if (owner and (not id(owner) or member ~= 1)) or (not owner and member == 1) then
    return uncertain('ownership evidence conflicts')
end
if state == 'RESERVED' then
    if owner ~= orderId or member ~= 1 then return uncertain('reserved ownership missing') end
    return 0 -- Recovery re-sends and cancelled orders must never deduct again.
end
if owner == orderId then return uncertain('ownership without RESERVED decision') end
if state == 'REJECTED' then
    local reason = redis.call('HGET', txn, 'reason')
    if reason == 'NO_STOCK' then return 1 end
    if reason == 'DUPLICATE' then return 2 end
    return uncertain('invalid rejection reason')
end
if state == 'ABORTED' then return 3 end
if state ~= 'PREPARED' then return uncertain('invalid transaction state') end
if now >= deadline then
    redis.call('HSET', txn, 'state', 'ABORTED', 'reason', 'PREPARE_TIMEOUT')
    return 3 -- Atomic with the same state checked by a late callback.
end
if action == 'PREPARE' then return 4 end
if action == 'CHECK' then return -1 end
if owner then
    redis.call('HSET', txn, 'state', 'REJECTED', 'reason', 'DUPLICATE', 'originalOrderId', owner)
    return 2
end
if kind(stockKey) ~= 'string' or redis.call('PTTL', stockKey) ~= -1 then return uncertain('invalid stock type or TTL') end
local stock = redis.call('GET', stockKey)
if (stock ~= '0' and not string.match(stock, '^[1-9]%d*$')) or #stock > 10 or tonumber(stock) > 2147483647 then
    return uncertain('invalid stock value')
end
if tonumber(stock) == 0 then
    redis.call('HSET', txn, 'state', 'REJECTED', 'reason', 'NO_STOCK')
    return 1
end
-- Validate before mutation. Runtime errors leave writePending=1: no abort/re-deduction.
redis.call('HSET', txn, 'writePending', '1')
redis.call('DECR', stockKey)
redis.call('SADD', buyers, userId)
redis.call('HSET', owners, userId, orderId)
redis.call('HSET', txn, 'state', 'RESERVED', 'reservedAt', now, 'writePending', '0')
return 0
