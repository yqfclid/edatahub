%%%-------------------------------------------------------------------
%% @doc edatahub top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edatahub_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-include("edatahub.hrl").
%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    ets:new(?DH_REG, [set, named_table, public, 
                     {write_concurrency,true}, {read_concurrency, true}]),
    DefaultPoolOpt = [{max_connections, 100}, {timeout, 60000}],
    PoolOpt = application:get_env(edatahub, http_pool, DefaultPoolOpt),
    UpdateInterval = application:get_env(edatahub, update_interval, 30000),
    SupFlags = 
        #{strategy => one_for_one,
          intensity => 10,
          period => 10},
    Proc1 = 
      #{id => edatahub_updater,              
        start => {edatahub_updater, start_link, [UpdateInterval]},
        restart => transient,
        shutdown => infinity,
        type => worker,
        modules => [edatahub_updater]},
    Proc2 = 
      #{id => edatahub_manager,              
        start => {edatahub_manager, start_link, [PoolOpt]},
        restart => transient,
        shutdown => infinity,
        type => worker,
        modules => [edatahub_manager]},  
    {ok, {SupFlags, [Proc1, Proc2]}}.

%%====================================================================
%% Internal functions
%%====================================================================
