-- KEYS: stock, claim-owner hash, purchaser set, permanent returned marker.
-- ARGV: userId, orderId, voucherId. Only the DB compensation worker calls this script.
-- 0 applied; 1 already applied; 2 missing/invalid stock; 3 invalid owner;
-- 4 invalid marker. Validate all predictable errors BEFORE performing any write.
local function kind(key) return redis.call('type', key).ok end
local stockKey, ownerKey, orderKey, markerKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local markerType = kind(markerKey)
local expected = ARGV[3] .. ':' .. ARGV[1] .. ':' .. ARGV[2]
if markerType ~= 'none' and markerType ~= 'string' then return 4 end
if markerType == 'string' then
    if redis.call('get', markerKey) ~= expected or redis.call('pttl', markerKey) ~= -1 then return 4 end
end
if kind(stockKey) ~= 'string' then return 2 end
local rawStock = redis.call('get', stockKey)
if not string.match(rawStock, '^%d+$') or #rawStock > 10 then return 2 end
local stock = tonumber(rawStock)
if stock > 2147483647 or redis.call('pttl', stockKey) ~= -1 then return 2 end
if kind(ownerKey) ~= 'hash' or kind(orderKey) ~= 'set' then return 3 end
if redis.call('hget', ownerKey, ARGV[1]) ~= ARGV[2]
    or redis.call('sismember', orderKey, ARGV[1]) ~= 1
    or redis.call('pttl', ownerKey) ~= -1 or redis.call('pttl', orderKey) ~= -1 then
    return 3
end
if markerType == 'string' then return 1 end
if stock >= 2147483647 then return 2 end
redis.call('incrby', stockKey, 1)
redis.call('set', markerKey, expected)
-- No TTL and no SREM/HDEL: cancelling does not restore purchasing eligibility.
return 0
