%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_proto).

-export([send_init/2, send_broadcast/3, send_request/5, send_reply/3,
	send_subscribe/2, send_publish/3, send_unsubscribe/2, send_close/1,
	send_tunnel_request/5, send_tunnel_reply/4, send_tunnel_data/3, send_tunnel_ack/2,
	send_tunnel_close/2, process/3, proc_init/1]).

% Packet opcodes
-define(OP_INIT,         16#00). % Out: connection initiation            | In: connection acceptance
-define(OP_DENY,         16#01). % Out: <never sent>                     | In: connection refusal
-define(OP_CLOSE,        16#02). % Out: connection tear-down initiation  | In: connection tear-down notification

-define(OP_BROADCAST,    16#03). % Out: application broadcast initiation | In: application broadcast delivery
-define(OP_REQUEST,      16#04). % Out: application request initiation   | In: application request delivery
-define(OP_REPLY,        16#05). % Out: application reply initiation     | In: application reply delivery

-define(OP_SUBSCRIBE,    16#06). % Out: topic subscription               | In: <never received>
-define(OP_UNSUBSCRIBE,  16#07). % Out: topic subscription removal       | In: <never received>
-define(OP_PUBLISH,      16#08). % Out: topic event publish              | In: topic event delivery

-define(OP_TUN_INIT,     16#09). % Out: tunnel construction request      | In: tunnel initiation
-define(OP_TUN_CONFIRM,  16#0a). % Out: tunnel confirmation              | In: tunnel construction result
-define(OP_TUN_ALLOW,    16#0b). % Out: tunnel transfer allowance        | In: <same as out>
-define(OP_TUN_TRANSFER, 16#0c). % Out: tunnel data exchange             | In: <same as out>
-define(OP_TUN_CLOSE,    16#0d). % Out: tunnel termination request       | In: tunnel termination notification

% Protocol constants
-define(PROTO_VERSION, "v1.0-draft2").
-define(CLIENT_MAGIC,  "iris-client-magic").
-define(RELAY_MAGIC,   "iris-relay-magic").

%% Serializes a boolean into an iolist.
-spec pack_boolean(Flag :: boolean()) ->
  Stream :: iolist().

pack_boolean(false) -> [0];
pack_boolean(true)  -> [1].

%% Serializes a variable int using base 128 encoding into an iolist.
-spec pack_varint(Num :: non_neg_integer()) ->
	Stream :: iolist().

pack_varint(Num) when Num < 128 -> Num;
pack_varint(Num)                -> [128 + Num rem 128, pack_varint(Num div 128)].

%% Serializes a length-tagged binary array into an iolist.
-spec pack_binary(Binary :: binary()) ->
	Stream :: iolist().

pack_binary(Binary) ->
	[pack_varint(byte_size(Binary)), Binary].

%% Serializes a length-tagged string into an iolist.
-spec pack_string(String :: [byte()]) ->
	Stream :: iolist().

pack_string(String) ->
	pack_binary(list_to_binary(String)).

%% Sends a connection initiation.
-spec send_init(Socket :: port(), Cluster :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_init(Socket, Cluster) ->
	gen_tcp:send(Socket, [?OP_INIT, pack_string(?CLIENT_MAGIC), pack_string(?PROTO_VERSION), pack_string(Cluster)]).

%% Assembles and serializes a broadcast packet into the relay.
-spec send_broadcast(Socket :: port(), App :: [byte()], Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_broadcast(Socket, App, Message) ->
	gen_tcp:send(Socket, [?OP_BROADCAST, pack_string(App), pack_binary(Message)]).

%% Assembles and serializes a request packet into the relay.
-spec send_request(Socket :: port(), ReqId :: non_neg_integer(), App :: [byte()],
	Req :: binary(), Timeout :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

send_request(Socket, ReqId, App, Req, Timeout) ->
	gen_tcp:send(Socket, [?OP_REQUEST, pack_varint(ReqId), pack_string(App), pack_binary(Req), pack_varint(Timeout)]).

%% Assembles and serializes a reply packet into the relay.
-spec send_reply(Socket :: port(), ReqId :: non_neg_integer(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

send_reply(Socket, ReqId, Reply) ->
	gen_tcp:send(Socket, [?OP_REPLY, pack_varint(ReqId),	pack_binary(Reply)]).

%% Assembles and serializes a subscription packet into the relay.
-spec send_subscribe(Socket :: port(), Topic :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_subscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_SUBSCRIBE, pack_string(Topic)]).

%% Assembles and serializes a message to be published in a topic.
-spec send_publish(Socket :: port(), Topic :: [byte()], Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_publish(Socket, Topic, Message) ->
	gen_tcp:send(Socket, [?OP_PUBLISH, pack_string(Topic), pack_binary(Message)]).

%% Assembles and serializes a subscription removal packet into the relay.
-spec send_unsubscribe(Socket :: port(), Topic :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_unsubscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_UNSUBSCRIBE, pack_string(Topic)]).

%% Assembles and serializes a close packet into the relay.
-spec send_close(Socket :: port()) ->
	ok | {error, Reason :: atom()}.

send_close(Socket) ->
	gen_tcp:send(Socket, [?OP_CLOSE]).

%% Assembles and serializes a tunneling packet into the relay.
-spec send_tunnel_request(Socket :: port(), TunId :: non_neg_integer(), App :: [byte()],
	Buffer :: pos_integer(), Timeout :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_request(Socket, TunId, App, Buffer, Timeout) ->
	gen_tcp:send(Socket, [?OP_TUN_INIT, pack_varint(TunId), pack_string(App), pack_varint(Buffer), pack_varint(Timeout)]).

%% Assembles and serializes a tunneling confirmation packet into the relay.
-spec send_tunnel_reply(Socket :: port(), TmpId :: non_neg_integer(),
	TunId :: non_neg_integer(), Buffer :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_reply(Socket, TmpId, TunId, Buffer) ->
	gen_tcp:send(Socket, [?OP_TUN_CONFIRM, pack_varint(TmpId), pack_varint(TunId), pack_varint(Buffer)]).

%% Assembles and serializes a tunnel data packet into the relay.
-spec send_tunnel_data(Socket :: port(), TunId :: non_neg_integer(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_data(Socket, TunId, Message) ->
	gen_tcp:send(Socket, [?OP_TUN_TRANSFER, pack_varint(TunId), pack_binary(Message)]).

%% Assembles and serializes a tunnel data acknowledgement into the relay.
-spec send_tunnel_ack(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_ack(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_ALLOW, pack_varint(TunId)]).

%% Assembles and serializes a tunnel atomination message into the relay.
-spec send_tunnel_close(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_close(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_CLOSE, pack_varint(TunId)]).

%% Retrieves a single byte from the relay connection.
-spec recv_byte(Socket :: port()) ->
	Input :: byte() | {error, Reason :: atom()}.

recv_byte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> Byte;
		Error            -> Error
	end.

%% Retrieves a boolean from the relay connection.
-spec recv_bool(Socket :: port()) ->
	Input :: boolean() | {error, Reason :: atom()}.

recv_bool(Socket) ->
	case recv_byte(Socket) of
    Error = {error, _Reason} -> Error;
		0 -> false;
		1 -> true;
		InvalidBool -> {error, io_lib:format("protocol violation: invalid boolean value: ~p", [InvalidBool])}
	end.

%% Retrieves a variable int in base 128 encoding from the relay connection.
-spec recv_varint(Socket :: port()) ->
	Input :: non_neg_integer() | {error, Reason :: atom()}.

recv_varint(Socket) ->
	case recv_byte(Socket) of
		Error = {error, _Reason} -> Error;
		Byte when Byte < 128     -> Byte;
		Byte ->
			case recv_varint(Socket) of
				Error = {error, _Reason} -> Error;
				Num                      -> Byte - 128 + 128 * Num
			end
	end.

%% Retrieves a length-tagged binary array from the relay connection.
-spec recv_binary(Socket :: port()) ->
	Input :: binary() | {error, Reason :: atom()}.

recv_binary(Socket) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> Binary;
				Error        -> Error
			end
	end.

%% Retrieves a length-tagged string from the relay connection.
-spec recv_string(Socket :: port()) ->
	Input :: [byte()] | {error, Reason :: atom()}.

recv_string(Socket) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> erlang:binary_to_list(Binary);
				Error        -> Error
			end
	end.

%% Retrieves a connection initiation response (either accept or deny).
-spec proc_init(Socket :: port()) ->
	{ok, Version :: string()} | {error, Reason :: atom()}.

proc_init(Socket) ->
  % Retrieve the response opcode
	case recv_byte(Socket) of
		Error = {error, _Reason} -> Error;
    OpCode when OpCode =:= ?OP_INIT; OpCode =:= ?OP_DENY ->
      % Verify the opcode validity and relay magic string
      case recv_string(Socket) of
        Error = {error, _Reason} -> Error;
        ?RELAY_MAGIC ->
          % Depending on success or failure, proceed and return
          case OpCode of
            ?OP_INIT -> recv_string(Socket);
            ?OP_DENY ->
              % Read the reason for connection denial
              case recv_string(Socket) of
                Error = {error, _Reason} -> Error;
                Reason -> {error, io_lib:format("connection denied: ~s", [Reason])}
              end
          end;
        InvalidMagic -> {error, io_lib:format("protocol violation: invalid relay magic: ~s", [InvalidMagic])}
      end;
		InvalidOpCode -> {error, io_lib:format("protocol violation: invalid init response opcode: ~p", [InvalidOpCode])}
	end.

%% Retrieves a remote broadcast message from the relay and notifies the handler.
-spec proc_broadcast(Socket :: port(), Handler :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_broadcast(Socket, Handler) ->
	case recv_binary(Socket) of
		Error = {error, _Reason} -> Error;
		Msg ->
			Handler ! {broadcast, Msg},
			ok
	end.

%% Retrieves a remote request from the relay.
-spec proc_request(Socket :: port(), Owner :: pid(), Handler :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_request(Socket, Owner, Handler) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recv_binary(Socket) of
				Error = {error, _Reason} -> Error;
				Req ->
					Handler ! {request, {Owner, ReqId}, Req},
					ok
			end
	end.

%% Retrieves a remote reply to a local request from the relay.
-spec proc_reply(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_reply(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recv_bool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {reply, ReqId, {error, timeout}},
					ok;
				false ->
					case recv_binary(Socket) of
						Error = {error, _Reason} -> Error;
						Reply ->
							Owner ! {reply, ReqId, {ok, Reply}},
							ok
					end
			end
	end.

%% Retrieves a topic event from the relay.
-spec proc_publish(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_publish(Socket, Owner) ->
	case recv_string(Socket) of
		Error = {error, _Reason} -> Error;
		Topic ->
			case recv_binary(Socket) of
				Error = {error, _Reason} -> Error;
				Event ->
					Owner ! {publish, Topic, Event},
					ok
			end
	end.

%% Retrieves a remote tunneling request from the relay.
-spec proc_tunnel_request(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_request(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		TmpId ->
			case recv_varint(Socket) of
				Error = {error, _Reason} -> Error;
				Buffer ->
					Owner ! {tunnel_request, TmpId, Buffer},
					ok
			end
	end.

%% Retrieves the remote reply to a local tunneling request from the relay.
-spec proc_tunnel_reply(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_reply(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recv_bool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {tunnel_reply, TunId, {error, timeout}},
					ok;
				false ->
					case recv_varint(Socket) of
						Error = {error, _Reason} -> Error;
						Buffer ->
							Owner ! {tunnel_reply, TunId, {ok, Buffer}},
							ok
					end
			end
	end.

%% Retrieves a remote tunnel data message.
-spec proc_tunnel_data(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_data(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recv_binary(Socket) of
				Error = {error, _Reason} -> Error;
				Msg ->
					Owner ! {tunnel_data, TunId, Msg},
					ok
			end
	end.

%% Retrieves a remote tunnel message acknowledgment.
-spec proc_tunnel_ack(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_ack(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_ack, TunId},
			ok
	end.

%% Retrieves the remote closure of a tunnel.
-spec proc_tunnel_close(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_close(Socket, Owner) ->
	case recv_varint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_close, TunId},
			ok
	end.

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
-spec process(Socket :: port(), Owner :: pid(), Handler :: pid()) ->
	no_return() | {error, Reason :: atom()}.

process(Socket, Owner, Handler) ->
	Res = case recv_byte(Socket) of
		Error = {error, _} -> Error;
		?OP_BROADCAST      -> proc_broadcast(Socket, Handler);
		?OP_REQUEST        -> proc_request(Socket, Owner, Handler);
		?OP_REPLY          -> proc_reply(Socket, Owner);
		?OP_PUBLISH        -> proc_publish(Socket, Owner);
		?OP_TUN_INIT       -> proc_tunnel_request(Socket, Owner);
		?OP_TUN_CONFIRM    -> proc_tunnel_reply(Socket, Owner);
    ?OP_TUN_ALLOW      -> proc_tunnel_ack(Socket, Owner);
		?OP_TUN_TRANSFER   -> proc_tunnel_data(Socket, Owner);
		?OP_TUN_CLOSE      -> proc_tunnel_close(Socket, Owner);
		?OP_CLOSE          -> exit(ok);
		_InvalidOpcode     -> {error, violation}
	end,
	case Res of
		ok              -> process(Socket, Owner, Handler);
		{error, Reason} -> exit(Reason)
	end.
