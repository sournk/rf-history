# FTP server installation

1. Install vsftp `apt-get install vsftpd libpam-pwdfile apache2-utils`.
2. Edit `/etc/vsftpd.conf`:
```
listen=YES
listen_ipv6=NO
anonymous_enable=NO
local_enable=YES # enable local and virtual users
write_enable=YES # enables to write
local_umask=022 # mask for new files
use_localtime=YES # server uses local time, no GMT
xferlog_enable=YES
xferlog_file=/var/log/vsftpd.log
xferlog_std_format=YES
connect_from_port_20=YES # best compability for old FTP clients
idle_session_timeout=600
data_connection_timeout=120
#ascii_upload_enable=YES
#ascii_download_enable=YES
chroot_local_user=YES
allow_writeable_chroot=YES
pasv_enable=YES # passive mode ON
pasv_min_port=62000
pasv_max_port=62999
guest_enable=YES # virtual users ON
guest_username=web
virtual_use_local_privs=YES # for virtual users aplly restriction of anon users
user_sub_token=$USER # template for user catalog
local_root=/var/www/$USER # user catalog
# hide_ids=YES # Hides for anon users owner of files
seccomp_sandbox=NO # For Ubuntu error solve: 500 OOPS: prctl PR_SET_SECCOMP failed 
```
3. Set up auth file `/etc/pam.d/vsftpd` running following 2 commands:
```
auth required pam_pwdfile.so pwdfile /etc/vsftpwd
account required pam_permit.so
```
4. Add new virtual user `htpasswd -d /etc/vsftpwd <username>`. If file there is no `/etc/vsftpwd file` use `htpasswd -c -d /etc/vsftpwd <username>` to create it.
5. Create user dir and give permissions:
```
mkdir /var/ftp/<username>
chown web:web /var/ftp/<username>
```
5. Delete user `htpasswd -D /etc/vsftpwd <username>`.
6. Start FTP service `service vsftpd restart`.
7. Update your UFW for FTP usage:
```
sudo ufw allow ftp
sudo ufw allow from any to any proto tcp port 62000:62999
```
8. Bot creates virtual FTP user, makes his dir using os commands, which require sudo password every time. To give for user `web` who runs Bot permission to run `htpasswd` without sudo pass make run `sudo visodo` and add `web    ALL=(ALL:ALL) NOPASSWD:/usr/bin/htpasswd` line.


[Настройка FTP](https://interface31.ru/tech_it/2016/01/nastraivaem-ftp-server-s-virtual-nymi-pol-zovatelyami-na-baze-vsftpd.html)

[Проблема UFW для PASSIVE MODE](https://askubuntu.com/questions/1222300/vsftp-and-ufw-problem)