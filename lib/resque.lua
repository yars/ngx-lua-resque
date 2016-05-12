local restyredis = require 'resty.redis'

local _M = {
    _VERSION = '0.5'
}


local mt = { __index = _M }


function _M.new(self, opts)
    return setmetatable({pool_size=10,
    host=nil, port=nil}, 
    mt)
end


function _M.redis(self, host, port)
	 self.host = host
	 self.port = port	 
end

function _enq(host, port, q_name, c_name, params, pool_size)
	 redis_server = restyredis:new()
	 redis_server:set_timeout(1000) -- 1 sec
	 local ok, err = redis_server:connect(host, port)
	 if ok
	    redis_server:rpush("resque:queue:"..q_name ,
                              cjson.encode( { class=c_name, args={params} } ))
            redis_server.set_keepalive(1000, pool_size)
	 end
	 return nil
end


function _M.enq_async(self, q_name, c_name, params)
	 ngx.timer.at(0, _enq, self.host, self.port, 
	 		 q_name, c_name, params,
			 self.pool_size)
end


function _M.enq(self, q_name, c_name, params)
   return _enq(self.host, self.port, 
	      q_name, c_name, params,
	      self.pool_size)
end

function _M.set_pool_size(self, psize)
	 self.pool_size = psize
end



return _M