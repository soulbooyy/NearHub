-- 滑动窗口限流：ZSET 存请求时间戳，窗口外淘汰后计数
-- KEYS[1] = rate:sw:{biz}:{userId}
-- ARGV[1] = nowMs
-- ARGV[2] = windowMs
-- ARGV[3] = maxRequests
-- ARGV[4] = member（唯一请求标识）
-- 返回：1 放行；0 限流
-- 窗口为 (nowMs - windowMs, nowMs]；仅记录放行请求，业务失败不退还额度。
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local maxReq = tonumber(ARGV[3])
local member = ARGV[4]
local minScore = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, minScore)
local count = redis.call('ZCARD', key)
if count >= maxReq then
    return 0
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return 1
