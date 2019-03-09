-record(dh_auth, {endpoint = <<>>,
                  access_id = <<>>,
                  access_key = <<>>}).

-record(dh_topic, {auth = #dh_auth{},
                   project = <<>>,
                   topic = <<>>,
                   comment = <<>>,
                   create_time = 0,
                   creator = <<>>,
                   last_modify_time = 0,
                   lifecycle = 0,
                   schema = #{},
                   record_type = <<"TUPLE">>,
                   shard_count = 0}).

-define(DH_REG, edatahub_reg).