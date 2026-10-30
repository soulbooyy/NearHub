-- KEYS: success marker string, stock string, purchaser set, owner hash.
-- ARGV: voucherId, userId, orderId, markerTtlSeconds.
-- 0 success; 1 no stock; 2 duplicate purchase; -1 uncertain/invalid evidence.
local txn, stockKey, buyers, owners = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local voucherId, userId, orderId, ttl = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local function id(value) return value and string.match(value, '^[1-9]%d*$') end
local function kind(key) return redis.call('TYPE', key).ok end
if not id(voucherId) or not id(userId) or not id(orderId) or not id(ttl)
    or #ttl > 10 or tonumber(ttl) > 2147483647 then return -1 end
local txnType, stockType, buyerType, ownerType = kind(txn), kind(stockKey), kind(buyers), kind(owners)
if (txnType ~= 'none' and txnType ~= 'string') or stockType ~= 'string'
    or redis.call('PTTL', stockKey) ~= -1
    or (buyerType ~= 'none' and (buyerType ~= 'set' or redis.call('PTTL', buyers) ~= -1))
    or (ownerType ~= 'none' and (ownerType ~= 'hash' or redis.call('PTTL', owners) ~= -1)) then return -1 end
local stock = redis.call('GET', stockKey)
if (stock ~= '0' and not string.match(stock, '^[1-9]%d*$')) or #stock > 10
    or tonumber(stock) > 2147483647 then return -1 end
local owner, member = redis.call('HGET', owners, userId), redis.call('SISMEMBER', buyers, userId)
if (owner and (not id(owner) or member ~= 1)) or (not owner and member == 1) then return -1 end
if txnType == 'string' then
    if redis.call('GET', txn) ~= '1' or redis.call('PTTL', txn) < 0 or owner ~= orderId then return -1 end
    return 0 -- Idempotent callback; do not extend the marker's lifetime.
end
if owner == orderId then return -1 end -- Expired/lost marker or partial write: never deduct again or recreate it.
if owner then return 2 end
if tonumber(stock) == 0 then return 1 end
-- All predictable validation precedes mutation. Lua execution is atomic, but runtime errors DO NOT undo writes.
redis.call('DECR', stockKey)
redis.call('SADD', buyers, userId)
redis.call('HSET', owners, userId, orderId)
redis.call('SET', txn, '1', 'EX', ttl)
return 0
