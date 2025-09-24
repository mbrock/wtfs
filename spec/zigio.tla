----------------------------- MODULE ReaderSpec -----------------------------
EXTENDS Naturals, Sequences

(*
  Constants
*)
CONSTANT Cap \in Nat                 \* interface buffer capacity
CONSTANT Src                         \* total, fixed source stream
ASSUME Src \in Seq(Nat) /\ \A b \in Src: b \in 0..255

(*
  State variables
    - Pos: start index (1-based) into Src for the beginning of Buf
    - Buf: bytes loaded from Src starting at Pos (length ≤ Cap)
    - Seek: how many bytes of Buf are already consumed (0..Len(Buf))
    - LastRead, LastDiscard: observable results of the last API step
*)
VARIABLES Pos, Buf, Seek, LastRead, LastDiscard

TypeInv ==
  /\ Pos \in 1..(Len(Src)+1)   \* allow Pos == Len(Src)+1 to mean “at end”
  /\ Buf \in Seq(Nat) /\ \A b \in Buf: b \in 0..255
  /\ 0 <= Seek /\ Seek <= Len(Buf)
  /\ Len(Buf) <= Cap
  /\ Buf = SubSeq(Src, Pos, Pos + Len(Buf) - 1)
  /\ LastRead \in Nat
  /\ LastDiscard \in Nat

Init ==
  /\ Pos = 1
  /\ Buf = << >>
  /\ Seek = 0
  /\ LastRead = 0
  /\ LastDiscard = 0

(*
  Derived helpers
*)
Buffered      == SubSeq(Buf, Seek+1, Len(Buf))      \* unread bytes currently buffered
BufferedLen   == Len(Buf) - Seek
AtEOS         == /\ BufferedLen = 0
                 /\ Pos + Len(Buf) - 1 = Len(Src)    \* no more bytes beyond the buffer tail

(*
  Rebase: slide unread bytes to the front (Seek -> 0).
  Default rebase does this when it needs more contiguous room.
*)
Rebase ==
  /\ Buf'  = Buffered
  /\ Pos'  = Pos + Seek
  /\ Seek' = 0
  /\ UNCHANGED <<LastRead, LastDiscard>>

(*
  Fill(n): ensure at least n bytes buffered (or reach EOS).
  We model one atomic attempt:
    - If already enough: no change.
    - Else, if there is space in Buf: read any x in 0..maxRead and append.
    - Else (no space but Seek > 0): Rebase, then a subsequent Fill can run.
    - If AtEOS and still insufficient: no progress (caller will see EndOfStream).
*)
Fill(n) ==
  \/ /\ BufferedLen >= n
     /\ UNCHANGED <<Pos, Buf, Seek, LastRead, LastDiscard>>
  \/ /\ BufferedLen < n /\ Len(Buf) < Cap
     /\ LET srcTail == SubSeq(Src, Pos + Len(Buf), Len(Src)) IN
        \E x \in 0..Min(Cap - Len(Buf), Len(srcTail)):
          /\ Buf'  = Buf \o Head(srcTail, x)
          /\ Pos'  = Pos
          /\ Seek' = Seek
          /\ UNCHANGED <<LastRead, LastDiscard>>
  \/ /\ BufferedLen < n /\ Len(Buf) = Cap /\ Seek > 0
     /\ Rebase
  \/ /\ BufferedLen < n /\ Len(Buf) = Cap /\ Seek = 0 /\ AtEOS
     /\ UNCHANGED <<Pos, Buf, Seek, LastRead, LastDiscard>>

(*
  Peek(n): requires Fill(n) succeeded; returns a view (no state change).
  We record LastRead = n for observability.
*)
Peek(n) ==
  /\ BufferedLen >= n
  /\ LastRead' = n
  /\ UNCHANGED <<Pos, Buf, Seek, LastDiscard>>

(*
  Toss(n): advance within the buffer (assert n <= BufferedLen).
*)
Toss(n) ==
  /\ n \in 0..BufferedLen
  /\ Seek' = Seek + n
  /\ UNCHANGED <<Pos, Buf, LastRead, LastDiscard>>

(*
  Take(n): Fill(n); then Peek(n); then Toss(n).
  Here we make it a single atomic step presuming Fill already ensured space.
*)
Take(n) ==
  /\ BufferedLen >= n
  /\ LastRead' = n
  /\ Seek' = Seek + n
  /\ UNCHANGED <<Pos, Buf, LastDiscard>>

(*
  DiscardShort(n): skip up to n bytes; may be short if EOS encountered.
  One atomic step:
    1) Use buffered bytes first.
    2) If more to discard and buffer empty, drop k bytes from the source tail,
       where k ∈ 0..available (short discards allowed).
*)
DiscardShort(n) ==
  LET use  == Min(n, BufferedLen) IN
  LET rem  == n - use IN
  LET pos0 == Pos + Seek + use IN
  LET avail == Max(0, Len(Src) - (pos0 - 1)) IN  \* remaining bytes in Src after buffered consumption
  \E k \in 0..Min(rem, avail):
    /\ Seek' = Seek + use
    /\ IF Seek' = Len(Buf)
         THEN /\ Pos' = pos0 + k
              /\ Buf' = << >>
              /\ Seek' = 0
         ELSE /\ Pos' = Pos
              /\ Buf' = Buf
    /\ LastDiscard' = use + k
    /\ UNCHANGED LastRead

(*
  DiscardAll(n) is the checked version: succeeds only if exactly n discarded.
  We model it as any DiscardShort(n) step where LastDiscard' = n.
*)
DiscardAll(n) == DiscardShort(n) /\ LastDiscard' = n

(*
  Next relation: pick one API action.
*)
Next ==
  \E n \in Nat:
      Fill(n) \/ Peek(n) \/ Take(n) \/ DiscardShort(n) \/ DiscardAll(n)
  \/ Rebase

==============================================================================
