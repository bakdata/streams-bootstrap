# rclone Helm for Kubernetes

## Usage:
- Create `rclone.conf` (either via `rclone configure` or from `rclone.conf.template`)
- Adapt `values.yaml` to contain your values.
- Install helm with: `helm install . -f values.yaml -n "mySource-mySoruceType-myDestType"` in this directory.

## Creating rclone.conf from scratch
- Download `rclone` (on Mac e.g. `brew install rclone`)
- Run the following commands:
```bash 
cd my-repository
rclone config --config "rclone.conf"
```
- Select `n` for "New remote"
- Enter name of source. Should be name of literature/patent source and match the directory name, e.g. `literature/cabi/rclone.conf` should have `cabi` as name.
- Select `10` for `ftp`.
- Enter ftp URL. Should not contain `ftp://`!
- Enter username.
- Enter port if different than 21. Otherwise just ENTER.
- Select `y` to add password.
- Add password twice.
- Select `n` to skip advanced settings.
- Select `y` to finish this config.

Now step one is done. Next we need to add S3.
- Select `n` for new remote.
- The name should be "s3" (without quotes)
- Select `4` for AWS-compliant sources and then `1` for AWS as the provider.
- Select `2` to use credentials from the environment (we use IAM)
- Press ENTER to leave credentials blank.
- Press ENTER to leave access key blank.
- Select `9` for EU Frankfurt region.
- Press ENTER to to leave default endpoint.
- Select `9` for EU region.
- Press ENTER to leave default ACL (== private).
- Press ENTER to leave default server-side encryption.
- Press ENTER to leave default ARN.
- Press ENTER to leave default storage class.
- Select `n` to skip advanced settings.
- Select `y` to finish source.
- Select `q` to quit config and verfiy that the config is correct.

Your config file should look like this now:
```
[my-source]
type = ftp
host = ftp.my-source.com
user = my-user
pass = 1FbNBhe7ndNsFcjTVofIarv1oP3d

[s3]
type = s3
provider = AWS
env_auth = true
region = eu-central-1
location_constraint = EU
```

Now we need to encrypt the file.
- `rclone config --config "rclone.conf"` to open config again.
- Select `s` to set configuration password.
- Select `a` to add own password.
- Enter password twice. Get the password from the gitlab environment variables.
- Select `q` to finish encryption.
- Select `q` to close config menu.

The config file is now complete and should look something like this:
```
# Encrypted rclone configuration File

RCLONE_ENCRYPT_V0:
nUebCNSIfjPZ3484t4Q2iSm252SkPCbMgheI1b//xegm0Hv2Fb6d4SkjgnGii6jy+dVK9f9NVhdoEcG/o4g4XHTdpHKl8GzDwhY90wZZIpNH1ID45XSVeDvRWAJSbqiLtoss4gEfCVCf/QvPWbcPon6ZGFGN8XszH2S7wQhnjL2Uwl1BrE6NwRurvoZ+Hsp6ml1E1mny8iI1WGopLPjkqJCk5GT6nJmy2YyLZbOsO7bs4rJy0p3GM4KBw6VuTza4+WKWRlJweKFo3gHB85Xa8xbBgpznOAx+MUfTKXyvkmMkZlVc37tC0rv4
```
