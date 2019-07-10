%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2019-03-04 20:54:57
%%%-------------------------------------------------------------------
-module(edatahub_rest).
-author("yqfclid").

-export([http_request/4]).
-export([convert_records/2]).
-export([common_headers/5]).

%%%===================================================================
%%% API
%%%===================================================================
http_request(Method, Url, Headers, Body) ->
    Optioins = get_options(Url),
    case hackney:request(Method, Url, Headers, Body, Optioins) of
        {ok, StatusCode, _RepHeaders, Ref} ->
            case hackney:body(Ref) of
                {ok, RtnBody} ->
                    case erlang:trunc(StatusCode/100) of
                        2 ->
                            {ok, RtnBody};
                        _ ->
                            {error, {StatusCode, RtnBody}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


convert_records(Schema, Records) ->
    Fields = maps:get(<<"fields">>, Schema, []),
    lists:map(
        fun(Record) ->
            EncodedValues = 
                lists:map(
                    fun(Field) ->
                        Name = maps:get(<<"name">>, Field),
                        RecordV = maps:get(Name, Record, null),
                        to_binary(RecordV)
                    end, Fields),
            #{<<"Data">> => EncodedValues}
    end, Records).

common_headers(Method, Path, AccessId, AccessKey, Body) ->
    Date = current_datetime(),
    MethodB = to_binary(Method),
    CanonicalBin = 
        <<MethodB/binary, 
          "\napplication/json\n", 
          Date/binary, 
          "\nx-datahub-client-version:1.1\n", 
          Path/binary>>,
    Authorization = sign_request(CanonicalBin, AccessId, AccessKey),
    Headers = 
        [{<<"Accept-Encoding">>, <<>>}, 
         {<<"Accept">>, <<"*/*">>}, 
         {<<"User-Agent">>, <<"wsnd">>},
         {<<"Connection">>, <<"keep-alive">>},
         {<<"Date">>, current_datetime()}, 
         {<<"Content-Type">>, <<"application/json">>}, 
         {<<"x-datahub-client-version">>, <<"1.1">>},
         {<<"Authorization">>, Authorization} 
        ],
    case MethodB of
        <<"GET">> ->
            Headers;
        <<"POST">> ->
            ExtraHeader = {<<"Content-Length">>, size(Body)},
            [ExtraHeader|Headers]
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_options(<<"https", _/binary>>) ->
    [
        {ssl_options, [{depth, 2}]},
        {pool, edatahub},
        {connect_timeout, 5000},
        {recv_timeout, 120000}
    ];
get_options(_) ->
    [
        {pool, edatahub},
        {connect_timeout, 5000},
        {recv_timeout, 120000}
    ]. 


to_binary(null) ->
    null;
to_binary(S) when is_atom(S) ->
    to_binary(atom_to_list(S));
to_binary(S) when is_float(S) ->
    float_to_binary(S);
to_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(S) when is_list(S) ->
    list_to_binary(S);
to_binary(S) when is_binary(S) ->
    S;
to_binary(S) ->
    erlang:error({bad_arg, S}).



sign_request(CanonicalBin, AccessId, AccessKey) ->
    Mac = crypto:hmac(sha, AccessKey, CanonicalBin),
    Sign = base64:encode(Mac),
    <<"DATAHUB ", AccessId/binary, ":", Sign/binary>>.    


current_datetime() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
    WeekDay = weekday(calendar:day_of_the_week(Year, Month, Day)),
    MonthFormat = month(Month),
    YearFormat = pad_int(Year),
    DayFormat = pad_int(Day),
    DateFormat = <<DayFormat/binary, " ", MonthFormat/binary, " ", YearFormat/binary>>,
    TimeFormat = list_to_binary(io_lib:format("~2.10.0B:~2.10.0B:~2.10.0B", [Hour, Min, Sec])),
    <<WeekDay/binary, ", ", DateFormat/binary, " ", TimeFormat/binary, " GMT">>.

pad_int(X) when X < 10 ->
    << $0, ($0 + X) >>;
pad_int(X) ->
    integer_to_binary(X).

weekday(1) -> <<"Mon">>;
weekday(2) -> <<"Tue">>;
weekday(3) -> <<"Wed">>;
weekday(4) -> <<"Thu">>;
weekday(5) -> <<"Fri">>;
weekday(6) -> <<"Sat">>;
weekday(7) -> <<"Sun">>.


month(1) -> <<"Jan">>;
month(2) -> <<"Feb">>;
month(3) -> <<"Mar">>;
month(4) -> <<"Apr">>;
month(5) -> <<"May">>;
month(6) -> <<"Jun">>;
month(7) -> <<"Jul">>;
month(8) -> <<"Aug">>;
month(9) -> <<"Sep">>;
month(10) -> <<"Oct">>;
month(11) -> <<"Nov">>;
month(12) -> <<"Dec">>.