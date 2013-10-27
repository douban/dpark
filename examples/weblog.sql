create table weblog(date, time, uid, ip, bid) from "/mfs/log/weblog";
$"def foo(x): return x[:4] + ';'";
select $"foo(date)" from weblog limit 1;
