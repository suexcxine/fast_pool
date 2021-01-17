-module(fast_pool_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  init_ets(),
  fast_pool_main_sup:start_link().

stop(_State) ->
    ok.

init_ets() ->
  ets:new(pools, [set, public, named_table, {keypos, 2}, 
    {read_concurrency, true}, {write_concurrency, true}]),

  % pool registry, pid to poolname, used to auto unregister when connection dies
  ets:new(pool_reverse, [set, public, named_table, 
    {read_concurrency, true}, {write_concurrency, true}]),

  % client pid to connection, used to sweep abnormal data
  ets:new(pool_use_reg, [set, public, named_table, {keypos, 2}, 
    {read_concurrency, true}, {write_concurrency, true}]),
  ok.
