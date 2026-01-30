local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = 1

local last_state = redis.call('HMGET', key, 'tokens', 'last_fill_time')
local tokens = tonumber(last_state[1]) or capacity
local last_fill_time = tonumber(last_state[2]) or now

local delta = math.max(0, now - last_fill_time)
local filled_tokens = math.min(capacity, tokens + (delta * refill_rate))

local allowed = false
local new_tokens = filled_tokens

if filled_tokens >= requested then
    allowed = true
    new_tokens = filled_tokens - requested
end

redis.call('HMSET', key, 'tokens', new_tokens, 'last_fill_time', now)
redis.call('EXPIRE', key, 60)

return allowed and 1 or 0
