%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2019-03-04 09:53:51
%%%-------------------------------------------------------------------
-module(edatahub).
-author("yqfclid").

-export([start/0]).
-export([list_project/0, list_project/1]).
-export([list_topic/1, list_topic/2]).
-export([wait_shards_ready/2, wait_shards_ready/3, wait_shards_ready/4]).
-export([list_shard/2, list_shard/3]).
-export([topic_info/2, topic_info/3]).
-export([put_records/2]).
-export([append_field/3]).
-export([append_connector_field/3]).

-export([reg_topic/2, reg_topic/3, reg_topic/4]).
-export([unreg_topic/1]).
-export([topic_info_by_reg/1]).
-export([wait_shards_ready_by_reg/1]).
-export([put_records_by_reg/2]).
-export([append_field_by_reg/3]).
-export([append_connector_field_by_reg/3]).

-export([default_auth/0]).


-include("edatahub.hrl").
%%%===================================================================
%%% API
%%%===================================================================
-spec start() -> {ok, list()} | {error, term()}.
start() ->
    application:ensure_all_started(edatahub).


-spec list_project() -> {ok, list()} | {error, term()}.
list_project() ->
    list_project(default_auth()).


-spec list_project(#dh_auth{}) -> {ok, list()} | {error, term()}.
list_project(DhAuth) ->
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Url = <<Endpoint/binary, "/projects">>,
    Headers = edatahub_rest:common_headers(<<"GET">>, <<"/projects">>, AccessId, AccessKey, <<>>),
    case edatahub_rest:http_request(get, Url, Headers, <<>>) of
        {ok, RtnBody} ->
            Projects = maps:get(<<"ProjectNames">>, jsx:decode(RtnBody, [return_maps]), []),
            {ok, Projects};
        {error, Reason} ->
            {error, Reason}
    end.

-spec list_topic(binary()) -> {ok, list()} | {error, term()}.
list_topic(Project) ->
    list_topic(default_auth(), Project).

-spec list_topic(#dh_auth{}, binary()) -> {ok, list()} | {error, term()}.
list_topic(DhAuth, Project) ->
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = <<"/projects/", Project/binary, "/topics">>,  
    Url = <<Endpoint/binary, Path/binary>>,
    Headers = edatahub_rest:common_headers(<<"GET">>, Path, AccessId, AccessKey, <<>>),
    case edatahub_rest:http_request(get, Url, Headers, <<>>) of
        {ok, RtnBody} ->
            Topics = maps:get(<<"TopicNames">>, jsx:decode(RtnBody, [return_maps]), []),
            {ok, Topics};
        {error, Reason} ->
            {error, Reason}
    end.

-spec wait_shards_ready(binary(), binary()) -> ok.
wait_shards_ready(Project, Topic) ->
    wait_shards_ready(default_auth(), Project, Topic).

-spec wait_shards_ready(#dh_auth{}, binary(), binary()) -> ok.
wait_shards_ready(DhAuth, Project, Topic) ->
    wait_shards_ready(DhAuth, Project, Topic, 30).

-spec wait_shards_ready(#dh_auth{}, binary(), binary(), integer()) -> ok.
wait_shards_ready(DhAuth, Project, Topic, Timeout) ->
    do_wait_shards_ready(DhAuth, Project, Topic, Timeout, os:system_time(seconds)).


-spec list_shard(binary(), binary()) -> {ok, list()} | {error, term()}.
list_shard(Project, Topic) ->
    list_shard(default_auth(), Project, Topic).

-spec list_shard(#dh_auth{}, binary(), binary()) -> {ok, list()} | {error, term()}.
list_shard(DhAuth, Project, Topic) ->
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = <<"/projects/", Project/binary, "/topics/", Topic/binary, "/shards">>,
    Url = <<Endpoint/binary, Path/binary>>,
    Headers = edatahub_rest:common_headers(<<"GET">>, Path, AccessId, AccessKey, <<>>),
    case edatahub_rest:http_request(get, Url, Headers, <<>>) of
        {ok, RtnBody} ->
            Decoded = jsx:decode(RtnBody, [return_maps]),
            {ok, maps:get(<<"Shards">>, Decoded, [])};
        {error, Reason} ->
            {error, Reason}
    end.

-spec topic_info(binary(), binary()) -> {ok, #dh_topic{}} | {error, term()}.
topic_info(Project, Topic) ->
    topic_info(default_auth(), Project, Topic).

-spec topic_info(#dh_auth{}, binary(), binary()) -> {ok, #dh_topic{}} | {error, term()}.
topic_info(DhAuth, Project, Topic) ->
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = <<"/projects/", Project/binary, "/topics/", Topic/binary>>,
    Url = <<Endpoint/binary, Path/binary>>,
    Headers = edatahub_rest:common_headers(<<"GET">>, Path, AccessId, AccessKey, <<>>),
    case edatahub_rest:http_request(get, Url, Headers, <<>>) of
        {ok, Body} ->
            #{<<"Comment">> := Comment,
              <<"CreateTime">> := CreateTime,
              <<"Creator">> := Creator,
              <<"LastModifyTime">> := LastModifyTime,
              <<"Lifecycle">> := Lifecycle,
              <<"RecordSchema">> := RawRecordSchema,
              <<"RecordType">> := RecordType,
              <<"ShardCount">> := ShardCount} = jsx:decode(Body, [return_maps]),
              RecordSchema = jsx:decode(RawRecordSchema, [return_maps]),
            TopicInfo = 
                #dh_topic{auth = DhAuth,
                          project = Project,
                          topic = Topic,
                          comment = Comment,
                          create_time = CreateTime,
                          creator = Creator,
                          last_modify_time = LastModifyTime,
                          lifecycle = Lifecycle,
                          schema = RecordSchema,
                          record_type = RecordType,
                          shard_count = ShardCount},
            {ok, TopicInfo};
        {error, Reason} ->
            {error, Reason}
    end.


-spec put_records(#dh_auth{}, list()) -> {ok, FailedCount :: integer(), FailedRecords :: list()} | {error, term()}.
put_records(DhTopic, Records) when is_record(DhTopic, dh_topic) ->
    #dh_topic{auth = DhAuth,
              project = Project,
              topic = Topic,
              schema = Schema} = DhTopic,
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = <<"/projects/", Project/binary, "/topics/", Topic/binary, "/shards">>,
    Url = <<Endpoint/binary, Path/binary>>,
    NRecords = edatahub_rest:convert_records(Schema, Records),
    Body = jsx:encode(#{<<"Action">> => <<"pub">>,
                        <<"Records">> => NRecords}),
    Headers = edatahub_rest:common_headers(<<"POST">>, Path, AccessId, AccessKey, Body),
    case edatahub_rest:http_request(post, Url, Headers, Body) of
        {ok, RtnBody} ->
            case catch jsx:decode(RtnBody, [return_maps]) of
                #{<<"FailedRecordCount">> := FailedCount,
                  <<"FailedRecords">> := FailedRecords} ->
                    {ok, FailedCount, FailedRecords};
                {'EXIT', Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
put_records(_, _) ->
    {error, badarg}.

%% fieldtype <<"bigint">> | <<"string">> | <<"boolean">> | <<"timestamp">> | <<"double">> | <<"decimal">>.
-spec append_field(#dh_auth{}, binary(), binary()) -> {ok, term()} | {error, term()}.
append_field(DhTopic, FieldName, FieldType) when is_record(DhTopic, dh_topic) ->
    #dh_topic{auth = DhAuth,
              project = Project,
              topic = Topic} = DhTopic,
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = Path = <<"/projects/", Project/binary, "/topics/", Topic/binary>>,
    Url = <<Endpoint/binary, Path/binary>>,
    Body = jsx:encode(#{<<"Action">> => <<"appendfield">>,
                        <<"FieldName">> => FieldName,
                        <<"FieldType">> => FieldType}),
    Headers = edatahub_rest:common_headers(<<"POST">>, Path, AccessId, AccessKey, Body),
    edatahub_rest:http_request(post, Url, Headers, Body);
append_field(_, _, _) ->
    {error, badarg}.

% connector type is <<"sink_odps">>, <<"sink_ads">>, <<"sink_es">>, <<"sink_fc">>, <<"sink_mysql">>, <<"sink_oss">>, <<"sink_ots">>.
-spec append_connector_field(#dh_auth{}, binary(), binary()) -> {ok, term()} | {error, term()}.
append_connector_field(DhTopic, ConnectorType, FieldName) when is_record(DhTopic, dh_topic) ->
    #dh_topic{auth = DhAuth,
              project = Project,
              topic = Topic} = DhTopic,
    #dh_auth{endpoint = Endpoint,
             access_id = AccessId,
             access_key = AccessKey} = DhAuth,
    Path = <<"/projects/", Project/binary, "/topics/", Topic/binary, "/connectors/", ConnectorType/binary>>,
    Url = <<Endpoint/binary, Path/binary>>,
    Body = jsx:encode(#{<<"Action">> => <<"appendfield">>,
                        <<"FieldName">> => FieldName}),
    Headers = edatahub_rest:common_headers(<<"POST">>, Path, AccessId, AccessKey, Body),
    edatahub_rest:http_request(post, Url, Headers, Body);
append_connector_field(_, _, _) ->
    {error, badarg}.


-spec reg_topic(any(), #dh_topic{}) -> ok.
reg_topic(RegName, TopicInfo) when is_record(TopicInfo, dh_topic) ->
    ets:insert_new(?DH_REG, {RegName, TopicInfo}),
    edatahub_updater:add_reg_topic(RegName).

-spec reg_topic(any(), binary(), binary()) -> ok.
reg_topic(RegName, Project, Topic) ->
    reg_topic(RegName, default_auth(), Project, Topic).

-spec reg_topic(any(), #dh_auth{}, binary(), binary()) -> ok.
reg_topic(RegName, DhAuth, Project, Topic) ->
    case topic_info(DhAuth, Project, Topic) of
        {ok, TopicInfo} ->
            reg_topic(RegName, TopicInfo);
        {error, Reason} ->
            {error, Reason}
    end.

-spec unreg_topic(any()) -> ok.
unreg_topic(RegName) ->
    ets:delete(?DH_REG, RegName),
    edatahub_updater:del_reg_topic(RegName).
    
-spec topic_info_by_reg(any()) -> {ok, term()} | {error, term()}.
topic_info_by_reg(RegName) ->
    case ets:lookup(?DH_REG, RegName) of
        [{RegName, TopicInfo}] ->
            #dh_topic{auth = DhAuth,
                      project = Project,
                      topic = Topic} = TopicInfo,
            topic_info(DhAuth, Project, Topic);
        [] ->
            {error, no_such_reg};
        {error, Reason} ->
            {error, Reason}
    end.

-spec wait_shards_ready_by_reg(any()) -> ok.
wait_shards_ready_by_reg(RegName) ->
    case ets:lookup(?DH_REG, RegName) of
        [{RegName, TopicInfo}] ->
            #dh_topic{auth = DhAuth,
                      project = Project,
                      topic = Topic} = TopicInfo,
            wait_shards_ready(DhAuth, Project, Topic);
        [] ->
            {error, no_such_reg};
        {error, Reason} ->
            {error, Reason}
    end.

-spec put_records_by_reg(any(), list()) -> {ok, term()} | {error, term()}.
put_records_by_reg(RegName, Records) ->
    case ets:lookup(?DH_REG, RegName) of
        [{RegName, TopicInfo}] ->
            put_records(TopicInfo, Records);
        [] ->
            {error, no_such_reg};
        {error, Reason} ->
            {error, Reason}
    end.

-spec append_field_by_reg(any(), binary(), binary()) -> {ok, term()} | {error, term()}.
append_field_by_reg(RegName, FieldName, FieldType) ->
    case ets:lookup(?DH_REG, RegName) of
        [{RegName, TopicInfo}] ->
            append_field(TopicInfo, FieldName, FieldType);
        [] ->
            {error, no_such_reg};
        {error, Reason} ->
            {error, Reason}
    end.

-spec append_connector_field_by_reg(any(), binary(), binary()) -> {ok, term()} | {error, term()}.
append_connector_field_by_reg(RegName, ConnectorType, FieldName) ->
    case ets:lookup(?DH_REG, RegName) of
        [{RegName, TopicInfo}] ->
            append_connector_field(TopicInfo, ConnectorType, FieldName);
        [] ->
            {error, no_such_reg};
        {error, Reason} ->
            {error, Reason}
    end.


-spec default_auth() -> #dh_auth{}.
default_auth() ->
    #dh_auth{endpoint = application:get_env(edatahub, endpoint, <<>>),
             access_id = application:get_env(edatahub, access_id, <<>>),
             access_key = application:get_env(edatahub, access_key, <<>>)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_wait_shards_ready(DhAuth, Project, Topic, Timeout, LastTime) ->
    do_wait_shards_ready(DhAuth, Project, Topic, Timeout, LastTime, Timeout + LastTime).

do_wait_shards_ready(DhAuth, Project, Topic, Timeout, LastTime, EndTime) ->
    case Timeout + LastTime > EndTime of
        true ->
            ok;
        false ->
            case list_shard(DhAuth, Project, Topic) of
                {ok, Shards} ->
                    case is_shards_all_ready(Shards) of
                        true ->
                            ok;
                        false ->
                            timer:sleep(1),
                            Now = os:system_time(seconds),
                            do_wait_shards_ready(DhAuth, Project, Topic, Timeout, Now, EndTime)
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

is_shards_all_ready(Shards) ->
    lists:all(
        fun(Shard) -> 
            State = map:get(<<"State">>, Shard, <<"CLOSE">>),
            lists:member(State, [<<"CLOSE">>, <<"ACTIVE">>])
    end, Shards).