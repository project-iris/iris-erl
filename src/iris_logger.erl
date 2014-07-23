%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the user configurable contextual logger.

%% private

-module(iris_logger).
-compile([{parse_transform, lager_transform}]).
-include_lib("lager/include/lager.hrl").

-export([new/0, new/1, new/2, crit/2, crit/3, error/2, error/3, warn/2, warn/3,
  info/2, info/3, debug/2, debug/3, level/1]).
-export_type([logger/0]).

-define(CONFIG_TABLE, iris_logger).


%% Contextual logger wrapper around basho's lager.
-type logger() :: {logger, term(), term()}.

-record(logger, {
  context,  %% Context associated with a logger
  flattened %% Context flattened for optimization
}).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Creates a new logger without any associated context.
new() -> #logger{context = [], flattened = ""}.

%% Creates a new logger with the specified associated context.
new(Context) ->
  #logger{context = Context, flattened = flatten(Context)}.

%% Creates a new logger by extending an existing one's context with additional
%% attributes specified.
new(#logger{context = Ctx, flattened = Flat}, Context) ->
  #logger{context = Ctx ++ Context, flattened = Flat ++ flatten(Context)}.


%% Logger functions for various log levels.
crit(Logger, Message)        -> log(critical, Logger, Message, []).
crit(Logger, Message, Attrs) -> log(critical, Logger, Message, Attrs).

error(Logger, Message)        -> log(error, Logger, Message, []).
error(Logger, Message, Attrs) -> log(error, Logger, Message, Attrs).

warn(Logger, Message)        -> log(warning, Logger, Message, []).
warn(Logger, Message, Attrs) -> log(warning, Logger, Message, Attrs).

info(Logger, Message)        -> log(info, Logger, Message, []).
info(Logger, Message, Attrs) -> log(info, Logger, Message, Attrs).

debug(Logger, Message)        -> log(debug, Logger, Message, []).
debug(Logger, Message, Attrs) -> log(debug, Logger, Message, Attrs).


%% Sets the level of the logs to be output.
level(none)  -> set_level(0);
level(Level) -> set_level(?LEVEL2NUM(Level)).


%% =============================================================================
%% Internal functions
%% =============================================================================

%% Sets the log level configuration to the value specified.
set_level(Level) ->
	try
		ets:new(?CONFIG_TABLE, [named_table, public, set, {keypos, 1}, {read_concurrency, true}])
	catch
		error:badarg -> ok % Config table already exists
	end,
	ets:insert(?CONFIG_TABLE, {level, Level}).


%% Enters a log entry into the lager ledger.
log(Level, #logger{context = Ctx, flattened = Pref}, Message, Attrs) ->
	% Fetch any local level filtering
	Log = try
		case ets:lookup(?CONFIG_TABLE, level) of
			[{level, LogLevel}] -> ?LEVEL2NUM(Level) =< LogLevel;
			[]                  -> ?LEVEL2NUM(Level) =< ?INFO
		end
	catch
		error:badarg -> ?LEVEL2NUM(Level) =< ?INFO % Config table doesn't exist yet
	end,

	% If locally not filtered, check lager filter
  case Log and (?SHOULD_LOG(Level)) of
    true  -> lager:log(Level, Ctx ++ Attrs, "~s", [format(Pref, Message, Attrs)]);
    false -> ok
  end.


%% Flattens a property list into a key-value assignments string.
flatten(Attributes) ->
  lists:foldl(fun({Key, Value}, Attrs) ->
    ValueRaw   = lists:flatten(io_lib:format("~p", [Value])),
    ValueFinal = case string:chr(ValueRaw, $\s) of
      0 -> string:strip(ValueRaw, both, $");
      _ -> ValueRaw
    end,
    io_lib:format("~s ~p=~s", [Attrs, Key, ValueFinal])
  end, "", Attributes).


%% Since basho's lager logger doesn't support printing the metadata associated
%% with log entries, override it and do it manually.
format(Context, {Format, Args}, Attributes) ->
  format(Context, lists:flatten(io_lib:format(Format, Args)), Attributes);

format(Context, Message, Attributes) ->
  io_lib:format("~-40s~s~s", [Message, Context, flatten(Attributes)]).
