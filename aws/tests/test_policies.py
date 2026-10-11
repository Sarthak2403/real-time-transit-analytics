import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from deploy import render, bundle


class PolicyTests(unittest.TestCase):
    def test_templates_are_scoped_and_valid(self):
        values={'ACCOUNT_ID':'123456789012','AWS_REGION':'us-east-1','S3_BUCKET':'transit-example'}
        for name in ('ec2','emr'):
            policy=json.loads(render(name+'-policy.json',values))
            for statement in policy['Statement']:
                resources=statement['Resource']
                resources=[resources] if isinstance(resources,str) else resources
                self.assertTrue(all(r.startswith('arn:aws:s3:::transit-example') for r in resources))
                actions=statement['Action']
                actions=[actions] if isinstance(actions,str) else actions
                self.assertNotIn('s3:*',actions)
            json.loads(render(name+'-trust.json',values))
        policy=json.loads(render('bucket-policy.json',values))
        self.assertEqual(policy['Statement'][0]['Effect'],'Deny')
        self.assertEqual(policy['Statement'][0]['Condition']['Bool']['aws:SecureTransport'],'false')

    def test_bundle_excludes_secrets_builds_and_state(self):
        import io,tarfile
        with tarfile.open(fileobj=io.BytesIO(bundle()),mode='r:gz') as archive:
            names=archive.getnames()
        self.assertIn('compose.yml',names)
        self.assertIn('aws/bootstrap.sh',names)
        for name in names:
            self.assertNotIn('.env',Path(name).parts)
            self.assertNotIn('.venv',Path(name).parts)
            self.assertNotIn('.state',Path(name).parts)
            self.assertNotIn('target',Path(name).parts)


if __name__ == '__main__':
    unittest.main()
