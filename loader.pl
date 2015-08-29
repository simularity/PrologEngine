% loader.pl - prolog loader interface

% Copyright 2015, Simularity, Inc.

:- module(loader, [engine_load_triples/2, engine_load_debug/0]).
:- use_module(library(engine_access)).

:- use_module(library(http/http_header)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).

% These are the predicates that get used by load_engine
:- thread_local segment/3, segment_segs/1, object_map/3, type_map/2, action_map/2,
	itemsize/2.

:- dynamic debug_mode/0.

:- meta_predicate engine_load_triples(+, 3).

engine_load_debug :-
    asserta(debug_mode).

% These are caching versions of the engine_access predicates for use in iterative
% loads
get_cached_type(Source, Name, Item) :-
	(type_map(Name, Item) ->
	 Item =..[_, Offset],
	 type_offset(Offset)
	;
	 engine_get_type(Source, Name, Item),
	 Item =.. [_, Offset], 
	 type_offset(Offset),
	 (integer(Name) ; assertz(type_map(Name, Item))), !).


get_cached_object(Source, Name, Type, Object) :-
    (object_map(Name, Type, Object) ->
	 writeln(test1(Object)),
	 item_value(Object),
	 writeln(test1)
	;
	engine_get_object(Source, Name, Type, Object),
	writeln(test2(Object)), 
	item_value(Object),
	writeln(test2),
	 (integer(Name) ; assertz(object_map(Name, Type, Object))), !).

get_cached_action(Source, Name, Action) :-
	(action_map(Name, Action) ->
	 true
	;
	 engine_get_action(Source, Name, Action),
	 (integer(Name) ; assertz(action_map(Name, Action))), !).

% This is the engine hash function
engine_hash(Atom, Hash) :-
	atom_codes(Atom, String),
	engine_hash(String, 5381, Hash).

engine_hash([], Hash, Hash).
engine_hash([V | T], Val, Hash) :-
	Next is (((Val << 5) + Val) + V) mod 0x7fffffffffffffff,
	engine_hash(T, Next, Hash).



clear_preds :-
	retractall(segment(_, _, _)),
	retractall(segment_segs(_)),
	retractall(object_map(_, _, _)),
	retractall(type_map(_, _)),
	retractall(action_map(_, _)).

% connect sets thread_local predicates that guide the loading
connect(Host:Port) :-
	clear_preds,
	format(atom(URL), 'http://~w:~w/segments', [Host, Port]),
	% This one doesn't have keep alive
	http_get(URL, JSON, []),
	atom_json_term(JSON, JTerm, []),
	JTerm = json(JSONList),
	member(segments=JList, JSONList),

	maplist(set_segment, JList),
	length(JList, NSegs),
	asserta(segment_segs(NSegs)), cached_get_item_size(Host:Port), !.

connect(_) :-
	throw(error(connect_failed), _).



cached_get_item_size(Engine) :-
    (itemsize(_, _) ->
	 true
     ;
     (get_item_size(Engine, Max, Min) ->
	  assertz(itemsize(Max, Min))
      ;
      asserta(itemsize(0, 0xffffffff)))).


% set_segment sets the tl predicate segment/2 which allows access to the segments 
set_segment(json(List)) :-
	member(segment=Seg, List),
	member(host=Host, List),
	member(sport=Sport, List),
	thread_create(segment_service(Host:Sport), TID, []),
	assertz(segment(Seg, Host:Sport, TID)).


segment_service(Host:Port) :-
	segment_service(Host:Port, 0, []).

segment_service(Host:Port, 10000, List) :-
	flush(Host:Port, List),!,
	segment_service(Host:Port, 0, []).

segment_service(Host:Port, M, List) :-
	thread_get_message(Message),
	(Message = terminate(Force) ->
	 (Force ; flush(Host:Port, List)) ,!
	;
	 Message = triple(_LHS, _Action, _RHS),
	 NextList = [Message | List],
	 N is M + 1,
	 segment_service(Host:Port, N, NextList)).

flush(Host:Port, List) :-
	format(atom(URL), 'http://~w:~w/load_data', [Host, Port]),
	write_to_codes(List, Codes),
	http_post(URL, codes(Codes), Result, []),
	atom_json_term(Result, JTerm, []),
	JTerm = json(JSONList),
	member(status=Status, JSONList),
	Status = 0, !.

flush(Source, _) :-
	throw(error(load_error(Source), flush/2)).

transform_subject(_Source, subject(Type, Item), subject(Type, Item)).
transform_subject(Source, subject(TimeLine, ContextType, Context, Stamp, Bits),
		  subject(TimeLine, Item)) :-

	get_cached_object(Source, Context, object(ContextType), CtxID),
	Mask is (1 << Bits) - 1,
	(Stamp =:= Stamp /\ Mask ->
	 true
	;
	 throw(error(stamp_overflow(Stamp, Bits), transform_subject/3))),
	Item is (CtxID << Bits) \/ Stamp,

	item_value(Item),
	

engine_load_(Host:Port, Closure) :-
	catch(connect(Host:Port), Ex, (clear_preds, throw(Ex))),
	cached_get_item_size(Host:Port),
	call(Closure, Subject, Action, Object),
	transform_subject(Host:Port, Subject, XFormSubj), 
	apply(Host:Port, XFormSubj, Action, Object),
	thread_exceptions,
	fail.

engine_load_(_, _) :-
	thread_exceptions, 
	term_threads(false),
	clear_preds.

engine_load_triples(Source, Closure) :-
	catch(engine_load_(Source, Closure), X,
	      (term_threads(true), clear_preds, throw(X))).

term_threads(Force) :-
	segment(_Seg, _Host, TID),
	(catch(thread_property(TID, status(running)), _, fail) ->
	 thread_send_message(TID, terminate(Force))
	;
	 true),
	thread_join(TID, _),
	fail.

term_threads(_).


thread_exceptions :-
	segment(Seg, Host, TID),
	thread_property(TID, status(exception(X))), !,
	throw(error(segment_thread(Seg, Host, X))).

thread_exceptions.

% Handle the Debug Mode Case
apply(_, Subject, Action, Object) :-
    debug_mode, !,
    format('triple(~q, ~q, ~q).~n', [Subject, Action, Object]).

apply(Host:Port, Subject, Action, Object) :-
	Subject = subject(ST, SI),
	Object = object(OT, OI),

	get_cached_type(Host:Port, subject(ST), subject(STN)),
	get_cached_type(Host:Port, object(OT), object(OTN)),
	get_cached_object(Host:Port, SI, subject(STN), SIN),
	get_cached_object(Host:Port, OI, object(OTN), OIN),
	get_cached_action(Host:Port, Action, AN),

% Convert the Items to strings to hash them into the segments
	atom_number(SINA, SIN),
	atom_number(OINA, OIN),
% Hash Them
	engine_hash(SINA, SINH),
	engine_hash(OINA, OINH),
% Convert to Segments
	segment_segs(Segs),
	SSEG is SINH mod Segs,
	OSEG is OINH mod Segs,

% Send it to the Segments
	apply_seg(OSEG, subject(STN, SIN), AN, object(OTN, OIN)),
	apply_seg(SSEG, object(OTN, OIN), AN, subject(STN, SIN)).

apply_seg(Seg, LHS, A, RHS) :-
	segment(Seg, _, TID),
	thread_send_message(TID, triple(LHS, A, RHS)).


type_offset(Value) :-
	Value =< 0x7fff,
	Value > 0, !.

type_offset(Value) :-
	type_error(sim_type, Value).

item_value(Value) :-

	itemsize(Min, Max),
	Value >= Min,
	Value =< Max, !.

item_value(Value) :-
	itemsize(Min, Max), 
	type_error(Min-Max, Value).

