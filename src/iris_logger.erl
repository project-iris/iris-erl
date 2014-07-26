%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @doc Module responsible for generating contextual log entries.
%%
%%      In order to prevent duplicating documentation, for an overview of the
%%      binding's logger functionality, please see <a target="_blank"
%%      href="https://github.com/project-iris/iris-erl#logging">Quickstart:
%%      Logger</a>.
%% @end

-module(iris_logger).
-compile([{parse_transform, lager_transform}]).
-include_lib("lager/include/lager.hrl").

-export([new/0, new/1, new/2, crit/2, crit/3, error/2, error/3, warn/2, warn/3,
  info/2, info/3, debug/2, debug/3, level/1]).
-export_type([logger/0]).


%% =============================================================================
%% Macros, types and records
%% =============================================================================

-define(CONFIG_TABLE, iris_logger).

-type logger() :: {logger, term(), term()}. %% Contextual logger wrapper around basho's lager.

-record(logger, {
  context,  %% Context associated with a logger
  flattened %% Context flattened for optimization
}).


%% =============================================================================
%% External API functions
%% =============================================================================

%% @doc Creates a new logger without any associated context.
%% @spec () -> Logger
%%      Logger = iris_logger:logger()
%% @end
-spec new() -> iris_logger:logger().

new() -> #logger{context = [], flattened = ""}.

%% @doc Creates a new logger with the specified associated context.
%% @spec (Context) -> Logger
%%      Context = [{Key, Value}]
%%        Key   = term()
%%        Value = term()
%%      Logger  = iris_logger:logger()
%% @end
-spec new(Context :: [{term(), term()}]) -> iris_logger:logger().

new(Context) ->
  #logger{context = Context, flattened = flatten(Context)}.

%% @doc Creates a new logger by extending an existing one's context with
%%      additional attributes specified.
%% @spec (Base, Context) -> Logger
%%      Base    = iris_logger:logger()
%%      Context = [{Key, Value}]
%%        Key   = term()
%%        Value = term()
%%      Logger  = iris_logger:logger()
%% @end
-spec new(Base :: iris_logger:logger(), Context :: [{term(), term()}]) ->
	iris_logger:logger().

new(#logger{context = Ctx, flattened = Flat}, Context) ->
  #logger{context = Ctx ++ Context, flattened = Flat ++ flatten(Context)}.


%% @doc Generates a critical log entry.
%% @spec (Logger, Message) -> ok
%%      Logger  = iris_logger:logger()
%%      Message = string()
%% @end
-spec crit(Logger :: iris_logger:logger(), Message :: string()) -> ok.

crit(Logger, Message) -> log(critical, Logger, Message, []).

%% @doc Generates a critical log entry with additional attributes.
%% @spec (Logger, Message, Attributes) -> ok
%%      Logger     = iris_logger:logger()
%%      Message    = string()
%%      Attributes = [Attribute]
%%        Attribute = [{Key, Value}]
%%          Key   = term()
%%          Value = term()
%% @end
-spec crit(Logger :: iris_logger:logger(), Message :: string(),
	Attributes :: [{term(), term()}]) -> ok.

crit(Logger, Message, Attributes) -> log(critical, Logger, Message, Attributes).


%% @doc Generates an error log entry.
%% @spec (Logger, Message) -> ok
%%      Logger  = iris_logger:logger()
%%      Message = string()
%% @end
-spec error(Logger :: iris_logger:logger(), Message :: string()) -> ok.

error(Logger, Message) -> log(error, Logger, Message, []).

%% @doc Generates an error log entry with additional attributes.
%% @spec (Logger, Message, Attributes) -> ok
%%      Logger     = iris_logger:logger()
%%      Message    = string()
%%      Attributes = [Attribute]
%%        Attribute = [{Key, Value}]
%%          Key   = term()
%%          Value = term()
%% @end
-spec error(Logger :: iris_logger:logger(), Message :: string(),
	Attributes :: [{term(), term()}]) -> ok.

error(Logger, Message, Attributes) -> log(error, Logger, Message, Attributes).


%% @doc Generates a warning log entry.
%% @spec (Logger, Message) -> ok
%%      Logger  = iris_logger:logger()
%%      Message = string()
%% @end
-spec warn(Logger :: iris_logger:logger(), Message :: string()) -> ok.

warn(Logger, Message)        -> log(warning, Logger, Message, []).

%% @doc Generates a warning log entry with additional attributes.
%% @spec (Logger, Message, Attributes) -> ok
%%      Logger     = iris_logger:logger()
%%      Message    = string()
%%      Attributes = [Attribute]
%%        Attribute = [{Key, Value}]
%%          Key   = term()
%%          Value = term()
%% @end
-spec warn(Logger :: iris_logger:logger(), Message :: string(),
	Attributes :: [{term(), term()}]) -> ok.

warn(Logger, Message, Attributes) -> log(warning, Logger, Message, Attributes).


%% @doc Generates an info log entry.
%% @spec (Logger, Message) -> ok
%%      Logger  = iris_logger:logger()
%%      Message = string()
%% @end
-spec info(Logger :: iris_logger:logger(), Message :: string()) -> ok.

info(Logger, Message)        -> log(info, Logger, Message, []).

%% @doc Generates an info log entry with additional attributes.
%% @spec (Logger, Message, Attributes) -> ok
%%      Logger     = iris_logger:logger()
%%      Message    = string()
%%      Attributes = [Attribute]
%%        Attribute = [{Key, Value}]
%%          Key   = term()
%%          Value = term()
%% @end
-spec info(Logger :: iris_logger:logger(), Message :: string(),
	Attributes :: [{term(), term()}]) -> ok.

info(Logger, Message, Attributes) -> log(info, Logger, Message, Attributes).


%% @doc Generates a debug log entry.
%% @spec (Logger, Message) -> ok
%%      Logger  = iris_logger:logger()
%%      Message = string()
%% @end
-spec debug(Logger :: iris_logger:logger(), Message :: string()) -> ok.

debug(Logger, Message)        -> log(debug, Logger, Message, []).

%% @doc Generates a debug log entry with additional attributes.
%% @spec (Logger, Message, Attributes) -> ok
%%      Logger     = iris_logger:logger()
%%      Message    = string()
%%      Attributes = [Attribute]
%%        Attribute = [{Key, Value}]
%%          Key   = term()
%%          Value = term()
%% @end
-spec debug(Logger :: iris_logger:logger(), Message :: string(),
	Attributes :: [{term(), term()}]) -> ok.

debug(Logger, Message, Attributes) -> log(debug, Logger, Message, Attributes).


%% @doc Sets the level of the logs to be output.
%% @spec (Level) -> ok
%%      Level = none | debug | info | warning | error | critical
%% @end
-spec level(Level :: none | debug | info | warning | error | critical) -> ok.

level(none)  -> set_level(0);
level(Level) -> set_level(?LEVEL2NUM(Level)).


%% =============================================================================
%% Internal functions
%% =============================================================================

%% Sets the log level configuration to the value specified.
-spec set_level(Level :: non_neg_integer()) -> ok.

set_level(Level) ->
	try
		ets:new(?CONFIG_TABLE, [named_table, public, set, {keypos, 1}, {read_concurrency, true}])
	catch
		error:badarg -> ok % Config table already exists
	end,
	ets:insert(?CONFIG_TABLE, {level, Level}),
	ok.


%% Enters a log entry into the lager ledger.
-spec log(Level :: debug | info | warning | error | critical, Logger :: iris_logger:logger(),
	Message :: string(), Arrts :: [{term(), term()}]) -> ok.

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
-spec flatten(Attributes :: [{term(), term()}]) -> iolist().

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
-spec format(Context :: string(), Message :: string(),
	Attributes :: [{term(), term()}]) -> iolist().

format(Context, Message, Attributes) ->
  io_lib:format("~-40s~s~s", [Message, Context, flatten(Attributes)]).
