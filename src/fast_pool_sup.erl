-module(fast_pool_sup).
-include("fast_pool.hrl").
-behaviour(supervisor).
-export([start_link/1, start_child/1]).
-export([init/1]).
-define(SERVER, ?MODULE).

start_link(InitArgs) ->
  Name = maps:get(poolname, InitArgs),
  supervisor:start_link({local, Name}, ?MODULE, InitArgs).

start_child(Poolname) ->
  supervisor:start_child(Poolname, []).

%% internal functions

init(#{mod := Mod,
       stop_mf := StopMF,
       poolname := Poolname,
       max_size := MaxSize,
       queue_limit := QueueLimit,
       args := Args}) ->
  ets:insert(pools, #pool_info{poolname = Poolname, 
                               cur_size = 0, 
                               max_size = MaxSize, 
                               last_used_pid = undefined, 
                               is_busy = false, 
                               mod = Mod, 
                               stop_mf = StopMF, 
                               queue_limit = QueueLimit}),
  ets:new(Poolname, [ordered_set, public, named_table, {keypos, 2}, 
    {read_concurrency, true}, {write_concurrency, true}]),
  SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
  % id field will be ignored by otp, so, whatever
  % https://erlang.org/doc/man/supervisor.html
  ChildSpecs = [#{id => 1, start => {Mod, start_link, [Args]}, shutdown => 3000}],
  {ok, {SupFlags, ChildSpecs}}.
