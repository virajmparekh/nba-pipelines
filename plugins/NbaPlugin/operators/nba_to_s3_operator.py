
from airflow.models import BaseOperator
from NbaPlugin.hooks.nba_hook import NbaHook


class NbaToS3Operator(BaseOperator):
    """
    Github To S3 Operator
    :param github_conn_id:           The Github connection id.
    :type github_conn_id:            string
    {insert github params here}
    :param s3_conn_id:               The s3 connection id.
    :type s3_conn_id:                string
    :param s3_bucket:                The S3 bucket to be used to store
                                     the Github data.
    :type s3_bucket:                 string
    :param s3_key:                   The S3 key to be used to store
                                     the Github data.
    :type s3_bucket:                 string
    """
    pass

    def __init__(self,
                 endpoint,
                 id,
                 method,
                 stats,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 payload=None,
                 *args,
                 **kwargs):
            super().__init__(*args, **kwargs)
            self.endpoint = endpoint
            self.id = id
            self.method = method
            self.stats = stats
            self.s3_conn_id = s3_conn_id
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.payload = payload

    def execute(self, context):

        print("WHAT IS GETTING PASSED HERE\n")
        print(self.endpoint)
        print(self.method)
        print(self.id)
        print(self.stats)

        print("WHAT IS GETTING PASSED HERE\n")
        print(type(self.endpoint))
        print(type(self.method))
        print(type(self.id))
        print(type(self.stats))

        response = NbaHook(self.endpoint, self.method,
                           str(self.id), self.stats)

        return response.call()




        # method = self.method
        #

        # m = {}
        #
        # for k, v in response.items():
        #     m[k] = v
        #
        # return m
