import boto3


def _get_resource(profile_name: str, region: str):
    session = boto3.Session(profile_name=profile_name)
    return session.resource('dynamodb',
                            region_name=region,
                            endpoint_url='http://localhost:8000' if region == 'localhost' else None)


