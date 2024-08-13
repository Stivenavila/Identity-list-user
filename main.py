import boto3
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError


def list_identity_center_users():
    sso_admin_client = boto3.client('sso-admin')
    identitystore_client = boto3.client('identitystore')

    response = sso_admin_client.list_instances()
    instance_arn = response['Instances'][0]['InstanceArn']
    identity_store_id = response['Instances'][0]['IdentityStoreId']

    users_data = []

    def get_user_info(user, user_number, total_users):
        user_id = user['UserId']
        print(f"Process user {user_number}/{total_users}: {user['UserName']} (UserId: {user_id})")

        user_info = {
            'UserId': user_id,
            'UserName': user['UserName'],
            'Email': user.get('Emails', [{'Value': 'N/A'}])[0]['Value'],
            'Groups': [],
            'AssignedAccounts': [],
            'AssignedPermissionSets': ""
        }

        groups = []
        group_paginator = identitystore_client.get_paginator('list_groups')
        for group_page in group_paginator.paginate(IdentityStoreId=identity_store_id):
            for group in group_page['Groups']:
                members = retry_api_call(identitystore_client.list_group_memberships,
                                         GroupId=group['GroupId'],
                                         IdentityStoreId=identity_store_id)['GroupMemberships']
                for member in members:
                    if member['MemberId']['UserId'] == user_id:
                        groups.append(group['DisplayName'])
        user_info['Groups'] = groups

        accounts_paginator = sso_admin_client.get_paginator('list_accounts_for_provisioned_permission_set')
        for ps_arn in sso_admin_client.list_permission_sets(InstanceArn=instance_arn)['PermissionSets']:
            ps_description = sso_admin_client.describe_permission_set(
                InstanceArn=instance_arn,
                PermissionSetArn=ps_arn
            )
            ps_name = ps_description['PermissionSet']['Name']

            for account_page in accounts_paginator.paginate(InstanceArn=instance_arn, PermissionSetArn=ps_arn):
                accounts = account_page['AccountIds']
                for account_id in accounts:
                    users_paginator = sso_admin_client.get_paginator('list_account_assignments')
                    for assignment_page in users_paginator.paginate(
                            InstanceArn=instance_arn, AccountId=account_id, PermissionSetArn=ps_arn
                    ):
                        for assignment in assignment_page['AccountAssignments']:
                            if assignment['PrincipalId'] == user_id:
                                if account_id not in user_info['AssignedAccounts']:
                                    user_info['AssignedAccounts'].append(account_id)
                                user_info['AssignedPermissionSets'] += (f"AccountId: {account_id}, "
                                                                        f"PermissionSetName: {ps_name};")
        user_info['AssignedPermissionSets'] = user_info['AssignedPermissionSets'].strip("; ")

        return user_info

    def retry_api_call(api_call, **kwargs):
        retries = 5
        for i in range(retries):
            try:
                return api_call(**kwargs)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ThrottlingException':
                    wait_time = 2 ** i
                    print(f"ThrottlingException: Esperando {wait_time} segundos antes de reintentar...")
                    time.sleep(wait_time)
                else:
                    raise e
        raise Exception(f"Error persistente después de {retries} intentos")

    paginator = identitystore_client.get_paginator('list_users')
    users = []
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        users.extend(page['Users'])

    total_users = len(users)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_user_info, user, idx + 1, total_users) for idx, user in enumerate(users)]
        for future in as_completed(futures):
            users_data.append(future.result())

    df = pd.DataFrame(users_data)
    return df
