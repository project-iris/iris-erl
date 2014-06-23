%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_proto).

-export([version/0]).
-export([send_init/2, send_broadcast/3, send_request/5, send_reply/3,
	send_subscribe/2, send_publish/3, send_unsubscribe/2, send_close/1,
	send_tunnel_request/5, send_tunnel_reply/4, send_tunnel_data/3, send_tunnel_ack/2,
	send_tunnel_close/2, process/3, proc_init/1]).

-define(OP_INIT,      16#00). %% Connection initialization
-define(OP_BCAST,     16#01). %% Application broadcast
-define(OP_REQ,       16#02). %% Application request
-define(OP_REP,       16#03). %% Application reply
-define(OP_SUB,       16#04). %% Topic subscription
-define(OP_PUB,       16#05). %% Topic publish
-define(OP_UNSUB,     16#06). %% Topic subscription removal
-define(OP_CLOSE,     16#07). %% Connection closing
-define(OP_TUN_REQ,   16#08). %% Tunnel building request
-define(OP_TUN_REP,   16#09). %% Tunnel building reply
-define(OP_TUN_DATA,  16#0a). %% Tunnel data transfer
-define(OP_TUN_ACK,   16#0b). %% Tunnel data acknowledgement
-define(OP_TUN_CLOSE, 16#0c). %% Tunnel closing

%% Implemented relay protocol version.
-spec version() -> [byte()].

version() -> "v1.0".

%% Serializes a variable int into an iolist.
-spec pack_varint(Num :: non_neg_integer()) ->
	Stream :: iolist().

pack_varint(Num) when Num < 128 -> Num;
pack_varint(Num)                -> [128 + Num rem 128, pack_varint(Num div 128)].

%% Serializes a length-tagged binary array into an iolist.
-spec pack_binary(Binary :: binary()) ->
	Stream :: iolist().

pack_binary(Binary) ->
	[pack_varint(byte_size(Binary)), Binary].

%% Serializes a length-tagged string into the relay.
-spec pack_string(String :: [byte()]) ->
	Stream :: iolist().

pack_string(String) ->
	pack_binary(list_to_binary(String)).

%% Assembles and serializes an init packet into the relay.
-spec send_init(Socket :: port(), App :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_init(Socket, App) ->
	gen_tcp:send(Socket, [?OP_INIT, pack_string(version()), pack_string(App)]).

%% Assembles and serializes a broadcast packet into the relay.
-spec send_broadcast(Socket :: port(), App :: [byte()], Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_broadcast(Socket, App, Message) ->
	gen_tcp:send(Socket, [?OP_BCAST, pack_string(App), pack_binary(Message)]).

%% Assembles and serializes a request packet into the relay.
-spec send_request(Socket :: port(), ReqId :: non_neg_integer(), App :: [byte()],
	Req :: binary(), Timeout :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

send_request(Socket, ReqId, App, Req, Timeout) ->
	gen_tcp:send(Socket, [?OP_REQ, pack_varint(ReqId), pack_string(App), pack_binary(Req), pack_varint(Timeout)]).

%% Assembles and serializes a reply packet into the relay.
-spec send_reply(Socket :: port(), ReqId :: non_neg_integer(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

send_reply(Socket, ReqId, Reply) ->
	gen_tcp:send(Socket, [?OP_REP, pack_varint(ReqId),	pack_binary(Reply)]).

%% Assembles and serializes a subscription packet into the relay.
-spec send_subscribe(Socket :: port(), Topic :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_subscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_SUB, pack_string(Topic)]).

%% Assembles and serializes a message to be published in a topic.
-spec send_publish(Socket :: port(), Topic :: [byte()], Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_publish(Socket, Topic, Message) ->
	gen_tcp:send(Socket, [?OP_PUB, pack_string(Topic), pack_binary(Message)]).

%% Assembles and serializes a subscription removal packet into the relay.
-spec send_unsubscribe(Socket :: port(), Topic :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_unsubscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_UNSUB, pack_string(Topic)]).

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
	gen_tcp:send(Socket, [?OP_TUN_REQ, pack_varint(TunId), pack_string(App), pack_varint(Buffer), pack_varint(Timeout)]).

%% Assembles and serializes a tunneling confirmation packet into the relay.
-spec send_tunnel_reply(Socket :: port(), TmpId :: non_neg_integer(),
	TunId :: non_neg_integer(), Buffer :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_reply(Socket, TmpId, TunId, Buffer) ->
	gen_tcp:send(Socket, [?OP_TUN_REP, pack_varint(TmpId), pack_varint(TunId), pack_varint(Buffer)]).

%% Assembles and serializes a tunnel data packet into the relay.
-spec send_tunnel_data(Socket :: port(), TunId :: non_neg_integer(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_data(Socket, TunId, Message) ->
	gen_tcp:send(Socket, [?OP_TUN_DATA, pack_varint(TunId), pack_binary(Message)]).

%% Assembles and serializes a tunnel data acknowledgement into the relay.
-spec send_tunnel_ack(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_ack(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_ACK, pack_varint(TunId)]).

%% Assembles and serializes a tunnel atomination message into the relay.
-spec send_tunnel_close(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

send_tunnel_close(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_CLOSE, pack_varint(TunId)]).

%% Retrieves a single byte from the relay.
-spec recv_byte(Socket :: port()) ->
	Input :: byte() | {error, Reason :: atom()}.

recv_byte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> Byte;
		Error            -> Error
	end.

%% Retrieves a boolean from the relay.
-spec recv_bool(Socket :: port()) ->
	Input :: boolean() | {error, Reason :: atom()}.

recv_bool(Socket) ->
	case recv_byte(Socket) of
		Error = {error, _Reason} -> Error;
		0 -> false;
		1 -> true;
		_InvalidBool -> {error, violation}
	end.

%% Retrieves a variable int from the relay.
-spec recvVarint(Socket :: port()) ->
	Input :: non_neg_integer() | {error, Reason :: atom()}.

recvVarint(Socket) ->
	case recv_byte(Socket) of
		Error = {error, _Reason} -> Error;
		Byte when Byte < 128     -> Byte;
		Byte ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Num                      -> Byte - 128 + 128 * Num
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
-spec recv_binary(Socket :: port()) ->
	Input :: binary() | {error, Reason :: atom()}.

recv_binary(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> Binary;
				Error        -> Error
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
-spec recv_string(Socket :: port()) ->
	Input :: [byte()] | {error, Reason :: atom()}.

recv_string(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> erlang:binary_to_list(Binary);
				Error        -> Error
			end
	end.

%% Retrieves a connection initialization response and returns whether ok.
-spec proc_init(Socket :: port()) ->
	ok | {error, Reason :: atom()}.

proc_init(Socket) ->
	case recv_byte(Socket) of
		?OP_INIT                 -> ok;
		Error = {error, _Reason} -> Error;
		_InvalidOpcode           -> {error, violation}
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
	case recvVarint(Socket) of
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
	case recvVarint(Socket) of
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
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TmpId ->
			case recvVarint(Socket) of
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
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recv_bool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {tunnel_reply, TunId, {error, timeout}},
					ok;
				false ->
					case recvVarint(Socket) of
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
	case recvVarint(Socket) of
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
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_ack, TunId},
			ok
	end.

%% Retrieves the remote closure of a tunnel.
-spec proc_tunnel_close(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

proc_tunnel_close(Socket, Owner) ->
	case recvVarint(Socket) of
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
		?OP_BCAST          -> proc_broadcast(Socket, Handler);
		?OP_REQ            -> proc_request(Socket, Owner, Handler);
		?OP_REP            -> proc_reply(Socket, Owner);
		?OP_PUB            -> proc_publish(Socket, Owner);
		?OP_TUN_REQ        -> proc_tunnel_request(Socket, Owner);
		?OP_TUN_REP        -> proc_tunnel_reply(Socket, Owner);
		?OP_TUN_DATA       -> proc_tunnel_data(Socket, Owner);
		?OP_TUN_ACK        -> proc_tunnel_ack(Socket, Owner);
		?OP_TUN_CLOSE      -> proc_tunnel_close(Socket, Owner);
		?OP_CLOSE          -> exit(ok);
		_InvalidOpcode     -> {error, violation}
	end,
	case Res of
		ok              -> process(Socket, Owner, Handler);
		{error, Reason} -> exit(Reason)
	end.
