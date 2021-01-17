-module(fast_pool_main_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).
-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_ref(), [supervisor:child_spec()]}}.
init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
  Sweeper = #{id => fast_pool_sweeper, start => {fast_pool_sweeper, start_link, []}, 
    restart => permanent, shutdown => 3000, type => worker, modules => [fast_pool_sweeper]},
  ChildSpecs = [Sweeper],
  {ok, {SupFlags, ChildSpecs}}.
