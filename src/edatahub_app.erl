%%%-------------------------------------------------------------------
%% @doc edatahub public API
%% @end
%%%-------------------------------------------------------------------

-module(edatahub_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    exometer:start(),
    application:set_env(hackney, mod_metrics, exometer),
    application:ensure_all_started(hackney),
    lager:start(),
    edatahub_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
