%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2019-03-05 16:17:24
%%%-------------------------------------------------------------------
-module(edatahub_manager).

-behaviour(gen_server).

%% API
-export([http_request_info/0,
         http_pool_info/0]).
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-include("edatahub.hrl").

-record(state, {pool_option}).

%%%===================================================================
%%% API
%%%===================================================================
http_request_info() ->
    {ok, Total} = exometer_info([hackney,total_requests], value),
    {ok, NB} = exometer_info([hackney,nb_requests], value),
    {ok, Finished} = exometer_info([hackney,finished_requests], value),
    [{total_requests, Total},
     {finished_requests, Finished},
     {nb_requests, NB}].

http_pool_info() ->
    PoolName = edatahub,
    {ok, TakeRate} = exometer:get_value([hackney_pool, PoolName, take_rate]),
    {ok, NoSocket} = exometer_info([hackney_pool, PoolName, no_socket], value),
    {ok, InUseCount} = exometer:get_value([hackney_pool, PoolName, in_use_count]),
    {ok, FreeCount} = exometer_info([hackney_pool, PoolName, free_count], value),
    {ok, QueueCounter} = exometer:get_value([hackney_pool, PoolName, queue_counter]),
    [{take_rate, TakeRate},
     {no_socket, NoSocket},
     {in_use_count, InUseCount},
     {free_count, FreeCount},
     {queue_counter, QueueCounter}].
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(PoolOption) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [PoolOption], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the SERVER
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([PoolOptioin]) ->
    case hackney_pool:start_pool(edatahub, PoolOptioin) of
        ok ->
            lager:info("[HTTP]Started edatahub pool"),
            ok;
        Error ->
            lager:warning("[HTTP]Start edatahub pool failed: ~p",
                          [Error])
    end,
    {ok, #state{pool_option = PoolOptioin}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call(_Request, _From, State) ->
    Reply = ok,
    lager:warning("Can't handle request: ~p", [_Request]),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    lager:warning("Can't handle msg: ~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    lager:warning("Can't handle info: ~p", [_Info]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
exometer_info(Name, Value) ->
    case exometer:get_value(Name, Value) of
        {ok, Prop} ->
            {ok, proplists:get_value(Value, Prop)};
        {error, Reason} ->
            {error, Reason}
    end.

to_string(S) when is_binary(S) ->
    binary_to_list(S);
to_string(S) when is_list(S) ->
    S;
to_string(S) when is_atom(S) ->
    atom_to_list(S);
to_string(S) ->
    erlang:throw({badarg, S}).