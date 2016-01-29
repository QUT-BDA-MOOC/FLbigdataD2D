register 'test.py' using jython as myfuncs;

numbers = load 'test.txt' as (a:int);

b = foreach numbers generate myfuncs.helloworld(), myfuncs.square(a);

dump b;
