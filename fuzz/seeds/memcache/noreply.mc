set key1 0 0 3 noreply
abc
add key2 0 0 3 noreply
def
replace key1 0 0 3 noreply
xyz
set counter 0 0 1 noreply
7
incr counter 1 noreply
decr counter 2 noreply
delete key2 noreply
get key1 counter
