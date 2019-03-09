%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2019-03-09 14:03:25
%%%-------------------------------------------------------------------
-module(edatahub_updater).

-behaviour(gen_server).

%% API
-export([add_reg_topic/1, del_reg_topic/1]).
-export([update/0]).
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

-record(state, {interval, time_ref, reg_topics = sets:new()}).

%%%===================================================================
%%% API
%%%===================================================================
add_reg_topic(RegName) ->
    gen_server:call(?SERVER, {add, RegName}).

del_reg_topic(RegName) ->
    gen_server:call(?SERVER, {del, RegName}).

update() ->
    gen_server:call(?SERVER, update).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Interval) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Interval], []).


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
init([Interval]) ->
    Timer = erlang:start_timer(Interval, self(), update),
    {ok, #state{interval = Interval, time_ref = Timer}}.

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
handle_call({add, RegTopic}, _From, #state{reg_topics = RegTopics} = State) ->
    NRegTopics = sets:add_element(RegTopic, RegTopics),
    {reply, ok, State#state{reg_topics = NRegTopics}};

handle_call({del, RegTopic}, _From, #state{reg_topics = RegTopics} = State) ->
    NRegTopics = sets:del_element(RegTopic, RegTopics),
    {reply, ok, State#state{reg_topics = NRegTopics}};

handle_call(update, _From, #state{reg_topics = RegTopics} = State) ->
    do_update(RegTopics),
    {reply, ok, State};

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
handle_info({timeout, Timer, update}, #state{time_ref = Timer} = State) ->
    #state{interval = Interval, reg_topics = RegTopics} = State, 
    do_update(RegTopics),   
    NTimer = erlang:start_timer(Interval, self(), update),
    {noreply, State#state{time_ref = NTimer}};

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
do_update(RegTopics) ->
    sets:fold(
        fun(RegTopic, _) ->
            case edatahub:topic_info_by_reg(RegTopic) of
                {ok, TopicInfo} ->
                    ets:insert(?DH_REG, {RegTopic, TopicInfo});
                {error, Reason} ->
                    lager:error("update reg topic ~p failed: ~p", [RegTopic, Reason])
            end
        end, ok, RegTopics).