-- KEYS: stock string, owner hash (userId -> originalOrderId).
-- ARGV: voucherId, userId, orderId.
-- 0 success; 1 no stock; 2 duplicate purchase; -1 uncertain/invalid evidence.
local stockKey, owners = KEYS[1], KEYS[2]
local voucherId, userId, orderId = ARGV[1], ARGV[2], ARGV[3]
local function id(value) return value and string.match(value, '^[1-9]%d*$') end
local function kind(key) return redis.call('TYPE', key).ok end
if not id(voucherId) or not id(userId) or not id(orderId) then return -1 end
local stockType, ownerType = kind(stockKey), kind(owners)
if stockType ~= 'string'
    or redis.call('PTTL', stockKey) ~= -1
    or (ownerType ~= 'none' and (ownerType ~= 'hash' or redis.call('PTTL', owners) ~= -1)) then return -1 end
local stock = redis.call('GET', stockKey)
if (stock ~= '0' and not string.match(stock, '^[1-9]%d*$')) or #stock > 10
    or tonumber(stock) > 2147483647 then return -1 end
local owner = redis.call('HGET', owners, userId)
if owner and not id(owner) then return -1 end
if owner == orderId then return 0 end -- The final write proves reservation; never deduct again.
if owner then return 2 end
if tonumber(stock) == 0 then return 1 end
-- All predictable validation precedes mutation. Lua execution is atomic, but runtime errors DO NOT undo writes.
redis.call('DECR', stockKey)
-- Sole reservation evidence: keep this as the LAST write, retain it after cancellation, never expire it.
redis.call('HSET', owners, userId, orderId)
return 0
