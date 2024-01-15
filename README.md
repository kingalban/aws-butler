# aws-butler
It's a hassle to navigate your infrastructure via the aws cli. 
You need something to connect the `log-group-names` for you.
That's exactly what _aws-butler_ will do for you!

Ok, so it's some scripts to get what you want from aws a bit faster. 

Installation and basic usage:

~~~
pip install aws-butler
~~~


~~~
# Authenticate with aws sso login ... or however you prefer

# List logs in a log-group
cloudwatch --profile <my-profile> --log-group-name log-gang ls

| name                                              | created at          | latest event at     | duration   |
|---------------------------------------------------+---------------------+---------------------+------------|
| my-ecs-task/work/3035f0c796ac4ac5ae77c0b5f4221386 | 2024-01-15 07:01:45 | 2024-01-01 07:04:56 | 0:01:35    |
| my-ecs-task/work/987282a0c6ba411c9da37922308a9e24 | 2024-01-15 03:01:06 | 2024-01-01 03:38:38 | 0:35:55    |
| my-ecs-task/work/82774690f65e454087083cb154d80c60 | 2024-01-15 03:00:52 | 2024-01-01 03:19:56 | 0:17:38    |
| my-ecs-task/work/058c8a018a634e3187db093542c75745 | 2024-01-15 03:01:01 | 2024-01-01 03:07:31 | 0:04:52    |
| my-ecs-task/work/ff443f4aaaa445beacb48425dca20db5 | 2024-01-15 03:00:36 | 2024-01-01 03:06:34 | 0:04:21    |
~~~

~~~

# Tail all logs in that group:
cloudwatch --profile <my-profile> --log-group-name log-gang tail 
#   Page through logs
#   ...
#   ...
~~~


~~~
# get parameters from ssm into a .env file

parameters --profile <my-profile> pull --path '/path/in/' > .env
~~~
