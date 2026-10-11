"""Explicit operator commands only: deploy, submit, status, cleanup."""
import argparse
import hashlib
import io
import json
import os
import shlex
import tarfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[1]


def render(name, values):
    return Template((ROOT / 'aws' / 'iam' / name).read_text()).substitute(values)


def document(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def missing(call, codes):
    from botocore.exceptions import ClientError
    try:
        return call()
    except ClientError as error:
        if error.response['Error']['Code'] not in codes:
            raise
        return None


def bundle():
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w:gz') as archive:
        for name in ('compose.yml', 'backend', 'simulator', 'aws'):
            target = ROOT / name
            files = [target] if target.is_file() else sorted(target.rglob('*'))
            for path in files:
                relative = path.relative_to(ROOT)
                if path.is_file() and not any(part in {'target', '__pycache__', '.venv', '.state'} for part in relative.parts):
                    archive.add(path, arcname=str(relative))
    return output.getvalue()


def ensure_role(iam, name, trust, policy, deployment):
    existing = missing(lambda: iam.get_role(RoleName=name), {'NoSuchEntity'})
    if existing is None:
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust, Tags=[{'Key':'TransitDeployment','Value':deployment}])
    else:
        if {'Key':'TransitDeployment','Value':deployment} not in existing['Role'].get('Tags',[]):
            raise ValueError('IAM role belongs to another deployment')
        iam.update_assume_role_policy(RoleName=name, PolicyDocument=trust)
    iam.put_role_policy(RoleName=name, PolicyName='transit-access', PolicyDocument=policy)


def deploy(args, session, state_path):
    from export_telemetry import parse_hour
    import ipaddress
    account = session.client('sts').get_caller_identity()['Account']
    ec2, s3, iam, emr = (session.client(name) for name in ('ec2', 's3', 'iam', 'emr-serverless'))
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    bucket = args.bucket
    expected = dict(account=account, region=args.region, bucket=bucket, name=args.name)
    if state and any(state.get(key) != value for key, value in expected.items()):
        raise ValueError('State belongs to a different deployment')
    network = ipaddress.ip_network(args.ssh_cidr, strict=True)
    if network.version != 4 or network.prefixlen != 32:
        raise ValueError('--ssh-cidr must be your IPv4 address with /32')
    if state.get('instance_id'):
        response = ec2.describe_instances(InstanceIds=[state['instance_id']])
        instance = response['Reservations'][0]['Instances'][0]
        if instance['State']['Name'] in ('shutting-down', 'terminated'):
            raise ValueError('Existing instance terminated; run cleanup before redeploying')
    else:
        instances = ec2.describe_instances(Filters=[{'Name':'tag:TransitDeployment','Values':[args.name]},
            {'Name':'instance-state-name','Values':['pending','running','stopping','stopped']}])
        found = [i for r in instances['Reservations'] for i in r['Instances']]
        if len(found) > 1:
            raise ValueError('Multiple tagged instances; resolve manually')
        instance = found[0] if found else None
    state.update(expected)
    state.setdefault('token', uuid.uuid4().hex)
    state.setdefault('export_start', args.export_start or datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0).isoformat())
    parse_hour(state['export_start'])
    document(state_path, state)
    # A globally unique bucket name is owned exclusively by this deployment.
    existing = missing(lambda: s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=account), {'404','NoSuchBucket'})
    if existing is None:
        options = {'Bucket':bucket}
        if args.region != 'us-east-1':
            options['CreateBucketConfiguration'] = {'LocationConstraint':args.region}
        s3.create_bucket(**options)
        s3.put_bucket_tagging(Bucket=bucket, Tagging={'TagSet':[{'Key':'TransitDeployment','Value':args.name}]})
    else:
        tags = missing(lambda: s3.get_bucket_tagging(Bucket=bucket), {'NoSuchTagSet'})
        if not tags or {'Key':'TransitDeployment','Value':args.name} not in tags['TagSet']:
            raise ValueError('Refusing to modify a bucket not tagged for this deployment')
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        key:True for key in ('BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets')})
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={'Rules':[
        {'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
    values = {'ACCOUNT_ID':account,'AWS_REGION':args.region,'S3_BUCKET':bucket}
    s3.put_bucket_policy(Bucket=bucket, Policy=render('bucket-policy.json',values))
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={'Rules':[
        {'ID':'expire-telemetry','Status':'Enabled','Filter':{'Prefix':'telemetry/'},'Expiration':{'Days':30}},
        {'ID':'expire-logs','Status':'Enabled','Filter':{'Prefix':'logs/'},'Expiration':{'Days':7}},
        {'ID':'expire-analytics','Status':'Enabled','Filter':{'Prefix':'analytics/'},'Expiration':{'Days':90}},
        {'ID':'abort-incomplete','Status':'Enabled','Filter':{'Prefix':''},'AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}}]})
    for kind in ('ec2','emr'):
        role = args.name + '-' + kind
        ensure_role(iam, role, render(kind+'-trust.json',values), render(kind+'-policy.json',values), args.name)
        state[kind+'_role'] = role
        document(state_path,state)
    profile = args.name + '-ec2'
    if missing(lambda: iam.get_instance_profile(InstanceProfileName=profile), {'NoSuchEntity'}) is None:
        iam.create_instance_profile(InstanceProfileName=profile,Tags=[{'Key':'TransitDeployment','Value':args.name}])
    profile_info = iam.get_instance_profile(InstanceProfileName=profile)['InstanceProfile']
    if {'Key':'TransitDeployment','Value':args.name} not in profile_info.get('Tags',[]):
        raise ValueError('Instance profile belongs to another deployment')
    roles = profile_info['Roles']
    if not roles:
        iam.add_role_to_instance_profile(InstanceProfileName=profile, RoleName=state['ec2_role'])
    elif roles[0]['RoleName'] != state['ec2_role']:
        raise ValueError('Instance profile contains an unexpected role')
    state['instance_profile'] = profile
    document(state_path,state)
    vpc = ec2.describe_subnets(SubnetIds=[args.subnet_id])['Subnets'][0]['VpcId']
    groups = ec2.describe_security_groups(Filters=[{'Name':'vpc-id','Values':[vpc]},
        {'Name':'group-name','Values':[args.name+'-ssh']}])['SecurityGroups']
    if groups:
        group = groups[0]['GroupId']
        if {'Key':'TransitDeployment','Value':args.name} not in groups[0].get('Tags',[]):
            raise ValueError('Security group belongs to another deployment')
    else:
        group = ec2.create_security_group(GroupName=args.name+'-ssh',Description='Transit demo SSH only',VpcId=vpc,
            TagSpecifications=[{'ResourceType':'security-group','Tags':[{'Key':'TransitDeployment','Value':args.name}]}])['GroupId']
    state['security_group_id'] = group
    document(state_path,state)
    permissions = ec2.describe_security_groups(GroupIds=[group])['SecurityGroups'][0]['IpPermissions']
    wanted = [{'IpProtocol':'tcp','FromPort':22,'ToPort':22,'IpRanges':[{'CidrIp':args.ssh_cidr}]}]
    if permissions != wanted:
        if permissions:
            ec2.revoke_security_group_ingress(GroupId=group,IpPermissions=permissions)
        ec2.authorize_security_group_ingress(GroupId=group,IpPermissions=wanted)
    s3.put_object(Bucket=bucket, Key='jobs/deployment.tar.gz', Body=bundle(), ServerSideEncryption='AES256')
    s3.upload_file(str(ROOT/'aws'/'route_analytics.py'),bucket,'jobs/route_analytics.py',ExtraArgs={'ServerSideEncryption':'AES256'})
    if instance is None:
        ami = args.ami_id or session.client('ssm').get_parameter(
            Name='/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64')['Parameter']['Value']
        settings = dict(S3_BUCKET=bucket,AWS_REGION=args.region,EXPORT_START=state['export_start'],DEPLOYMENT_KEY='jobs/deployment.tar.gz')
        user_data = '#!/bin/bash\nset -euo pipefail\n' + ''.join('export '+k+'='+shlex.quote(v)+'\n' for k,v in settings.items())
        user_data += (ROOT/'aws'/'bootstrap.sh').read_text()
        # IAM propagation can lag profile creation. Retry only the propagation error.
        for attempt in range(12):
            try:
                instance = ec2.run_instances(ImageId=ami,InstanceType='t3.large',MinCount=1,MaxCount=1,
                    KeyName=args.key_name,IamInstanceProfile={'Name':profile},
                    NetworkInterfaces=[{'DeviceIndex':0,'SubnetId':args.subnet_id,'Groups':[group],'AssociatePublicIpAddress':True}],
                    MetadataOptions={'HttpTokens':'required','HttpEndpoint':'enabled'},
                    BlockDeviceMappings=[{'DeviceName':'/dev/xvda','Ebs':{'VolumeSize':100,'VolumeType':'gp3','Encrypted':True,'DeleteOnTermination':True}}],
                    CreditSpecification={'CpuCredits':'standard'},
                    TagSpecifications=[{'ResourceType':kind,'Tags':[{'Key':'Name','Value':args.name},{'Key':'TransitDeployment','Value':args.name}]} for kind in ('instance','volume')],
                    UserData=user_data,ClientToken=hashlib.sha256((state['token']+'-ec2').encode()).hexdigest())['Instances'][0]
                break
            except Exception as error:
                if attempt == 11 or 'Invalid IAM Instance Profile' not in str(error):
                    raise
                time.sleep(5)
    state['instance_id'] = instance['InstanceId']
    document(state_path,state)
    if instance['State']['Name'] == 'stopped':
        ec2.start_instances(InstanceIds=[state['instance_id']])
    ec2.get_waiter('instance_running').wait(InstanceIds=[state['instance_id']])
    state['public_ip'] = ec2.describe_instances(InstanceIds=[state['instance_id']])['Reservations'][0]['Instances'][0].get('PublicIpAddress')
    document(state_path,state)
    apps = []
    for page in emr.get_paginator('list_applications').paginate():
        apps.extend(a for a in page['applications'] if a['name'] == args.name and a['state'] != 'TERMINATED')
    if len(apps) > 1:
        raise ValueError('Multiple EMR applications with this name')
    if apps:
        application_id = apps[0]['id']
        application = emr.get_application(applicationId=application_id)['application']
        if application.get('tags',{}).get('TransitDeployment') != args.name:
            raise ValueError('EMR application belongs to another deployment')
    else:
        application_id = emr.create_application(name=args.name,releaseLabel=args.emr_release,type='SPARK',clientToken=state['token'],
            maximumCapacity={'cpu':'4 vCPU','memory':'16 GB'},
            autoStartConfiguration={'enabled':True},autoStopConfiguration={'enabled':True,'idleTimeoutMinutes':1},
            tags={'TransitDeployment':args.name})['applicationId']
    state['application_id'] = application_id
    document(state_path,state)
    iam.update_assume_role_policy(RoleName=state['emr_role'],
        PolicyDocument=render('emr-trust.json',values).replace('/applications/*','/applications/'+application_id))
    print(json.dumps(state,indent=2))
    print('Provisioned only. Bootstrap runs on EC2; verify cloud-init and service health. No Spark job submitted.')
    print('Existing instances are reused; source uploads do not automatically update their checkout.')


def submit(args, session, state):
    from datetime import date
    date.fromisoformat(args.date)
    emr = session.client('emr-serverless')
    response = emr.start_job_run(applicationId=state['application_id'],
        executionRoleArn=f"arn:aws:iam::{state['account']}:role/{state['emr_role']}",
        name='routes-'+args.date,executionTimeoutMinutes=30,
        jobDriver={'sparkSubmit':{'entryPoint':f"s3://{state['bucket']}/jobs/route_analytics.py",
            'entryPointArguments':['--input',f"s3://{state['bucket']}/telemetry/",'--output',f"s3://{state['bucket']}/analytics/",'--date',args.date],
            'sparkSubmitParameters':'--conf spark.driver.cores=2 --conf spark.driver.memory=6g --conf spark.executor.cores=2 --conf spark.executor.memory=6g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.sql.shuffle.partitions=8'}},
        configurationOverrides={'monitoringConfiguration':{'s3MonitoringConfiguration':{'logUri':f"s3://{state['bucket']}/logs/"}}})
    print('Job run:',response['jobRunId'])


def cleanup(args, session, state, state_path):
    if not args.confirm:
        raise ValueError('Cleanup terminates EC2 and deletes its data disk; pass --confirm')
    ec2, emr, iam, s3 = (session.client(name) for name in ('ec2','emr-serverless','iam','s3'))
    application_id = state.get('application_id')
    if application_id:
        application = missing(lambda: emr.get_application(applicationId=application_id), {'ResourceNotFoundException'})
        if application:
            active = []
            for page in emr.get_paginator('list_job_runs').paginate(applicationId=application_id):
                active.extend(j for j in page['jobRuns'] if j['state'] not in ('SUCCESS','FAILED','CANCELLED'))
            for job in active:
                emr.cancel_job_run(applicationId=application_id,jobRunId=job['id'])
            for _ in range(60):
                info = emr.get_application(applicationId=application_id)['application']
                if info['state'] in ('CREATED','STOPPED','TERMINATED'):
                    break
                if info['state'] == 'STARTED':
                    missing(lambda: emr.stop_application(applicationId=application_id), {'ValidationException'})
                time.sleep(5)
            else:
                raise RuntimeError('EMR did not stop; rerun cleanup; resources may remain billable')
            if info['state'] != 'TERMINATED':
                emr.delete_application(applicationId=application_id)
    if state.get('instance_id'):
        present = missing(lambda: ec2.describe_instances(InstanceIds=[state['instance_id']]), {'InvalidInstanceID.NotFound'})
        if present:
            ec2.terminate_instances(InstanceIds=[state['instance_id']])
            ec2.get_waiter('instance_terminated').wait(InstanceIds=[state['instance_id']])
    if state.get('security_group_id'):
        for attempt in range(12):
            try:
                missing(lambda: ec2.delete_security_group(GroupId=state['security_group_id']), {'InvalidGroup.NotFound'})
                break
            except Exception:
                if attempt == 11:
                    raise
                time.sleep(5)
    profile = state.get('instance_profile')
    if profile:
        found = missing(lambda: iam.get_instance_profile(InstanceProfileName=profile), {'NoSuchEntity'})
        if found:
            for role in found['InstanceProfile']['Roles']:
                iam.remove_role_from_instance_profile(InstanceProfileName=profile,RoleName=role['RoleName'])
            iam.delete_instance_profile(InstanceProfileName=profile)
    for kind in ('ec2','emr'):
        if state.get(kind+'_role'):
            role = state[kind+'_role']
            missing(lambda: iam.delete_role_policy(RoleName=role,PolicyName='transit-access'), {'NoSuchEntity'})
            missing(lambda: iam.delete_role(RoleName=role), {'NoSuchEntity'})
    if args.delete_bucket:
        found = missing(lambda: s3.head_bucket(Bucket=state['bucket'],ExpectedBucketOwner=state['account']), {'404','NoSuchBucket'})
        if found is not None:
            tags = s3.get_bucket_tagging(Bucket=state['bucket'])['TagSet']
            if {'Key':'TransitDeployment','Value':state['name']} not in tags:
                raise ValueError('Bucket ownership tag mismatch')
            versions = s3.get_bucket_versioning(Bucket=state['bucket']).get('Status')
            if versions:
                raise ValueError('Bucket versioning changed; remove versions manually before cleanup')
            for page in s3.get_paginator('list_multipart_uploads').paginate(Bucket=state['bucket']):
                for upload in page.get('Uploads',[]):
                    s3.abort_multipart_upload(Bucket=state['bucket'],Key=upload['Key'],UploadId=upload['UploadId'])
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=state['bucket']):
                objects = [{'Key':obj['Key']} for obj in page.get('Contents',[])]
                if objects:
                    result = s3.delete_objects(Bucket=state['bucket'],Delete={'Objects':objects})
                    if result.get('Errors'):
                        raise RuntimeError('Some S3 objects could not be deleted')
            s3.delete_bucket(Bucket=state['bucket'])
    document(state_path, dict(state, compute_removed=True))
    print('Compute removed. '+('Bucket deleted.' if args.delete_bucket else 'S3 retained and still billable; rerun with --delete-bucket to remove it.'))


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', default=os.getenv('AWS_REGION','us-east-1'))
    parser.add_argument('--state',type=Path,default=ROOT/'aws'/'.state'/'deployment.json')
    commands = parser.add_subparsers(dest='command',required=True)
    deployment = commands.add_parser('deploy')
    deployment.add_argument('--name',default='transit-demo')
    for option in ('bucket','subnet-id','key-name','ssh-cidr'):
        deployment.add_argument('--'+option,required=True)
    deployment.add_argument('--ami-id')
    deployment.add_argument('--export-start')
    deployment.add_argument('--emr-release',default='emr-7.9.0')
    job = commands.add_parser('submit')
    job.add_argument('--date',required=True)
    commands.add_parser('status')
    removal = commands.add_parser('cleanup')
    removal.add_argument('--confirm',action='store_true')
    removal.add_argument('--delete-bucket',action='store_true')
    args = parser.parse_args()
    session = boto3.Session(region_name=args.region)
    if args.command == 'deploy':
        if not args.name.replace('-','').isalnum() or len(args.name)>40:
            parser.error('name must contain letters, digits or hyphens and be <=40 characters')
        deploy(args,session,args.state)
    else:
        state = json.loads(args.state.read_text())
        account = session.client('sts').get_caller_identity()['Account']
        if state['account'] != account or state['region'] != args.region:
            raise ValueError('AWS account/region does not match deployment state')
        if args.command == 'submit':
            submit(args,session,state)
        elif args.command == 'cleanup':
            cleanup(args,session,state,args.state)
        else:
            print(json.dumps(state,indent=2))
            if state.get('instance_id'):
                print(session.client('ec2').describe_instances(InstanceIds=[state['instance_id']])['Reservations'][0]['Instances'][0]['State'])
            if state.get('application_id'):
                info = missing(lambda: session.client('emr-serverless').get_application(applicationId=state['application_id']), {'ResourceNotFoundException'})
                print(info['application']['state'] if info else 'EMR application deleted')


if __name__ == '__main__':
    main()
