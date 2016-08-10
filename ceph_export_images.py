# -*- coding:utf-8 -*-
import time,commands

backup_images={
    # uuid: {name:xxxx,local:/mnt/backup/1}
    # 'd74924e0-3047-4df5-a0e7-791389af87b5_disk':{'name':'ceph_backup_test','local':'/mnt/backup/1'},
    # '5d26a114-db75-496e-aa23-e7844a36d0b1_disk':{'name':'wx','local':'/mnt/backup/4'},


}

"""
1, 预先export raw格式到/tmpdir
2, 执行pigz压缩镜像到目标储存位置
        tar -I pigz -cf tarball.tgz files
    或   tar -czf tarball.tgz files
3, 清除/tmpdir的文件

"""

ceph_pool = 'compute'
tmp_dir = '/tmp'

# 1  gen_export_img_tmp('5d26a114-db75-496e-aa23-e7844a36d0b1_disk','/tmp/2016-07-27_5d26a1_wx.img')
def gen_export_img_tmp(uuid,tmp_image):
    ceph_export_template = "rbd export -p %s --image  %s  %s"%(ceph_pool,'%s','%s')
    # current_datetime = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    uuid = uuid
    return ceph_export_template%(uuid,tmp_image)


#2
def compress_img(tmp_image,opt_image_tgz):
    fo = open("backup.log", "a")
    log_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    try:
        compress_command = 'tar -I pigz -cf %s %s'%(opt_image_tgz,tmp_image)
        status,out=commands.getstatusoutput(compress_command)
        if not status:
            log_msg = "%s : INFO  多进程压缩 %s  成功 \n"%(log_time,opt_image_tgz)
        else:
            log_msg = "%s : WAR 可能缺少pigz : %s 尝试使用单线程进行压缩\n"%(log_time,str(out))
            singlecompress_command = "tar -czf %s %s"%(opt_image_tgz,tmp_image)
            status,out=commands.getstatusoutput(singlecompress_command)
            if not status:
                log_msg =log_msg+ "%s : INFO  单进程压缩 %s  成功 \n"%(log_time,opt_image_tgz)
                fo.write(log_msg)
                fo.close()
                return 1
            else:
                log_msg = log_msg+"%s : INFO  单进程压缩 %s  失败:%s \n"%(log_time,opt_image_tgz,str(out))
                fo.write(log_msg)
                fo.close()
                return -1

        fo.write(log_msg)
        fo.close()
        return 0

    except Exception as e:
        log_msg = "%s : ERROR 压缩镜像失败 : %s \n"%(log_time,str(e))
        fo.write(log_msg)
        fo.close()
        return -1


#3
def rm_tmp_image(tmp_image):
    fo = open("backup.log", "a")
    log_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    rm_command = 'rm %s'%(tmp_image)
    status,out=commands.getstatusoutput(rm_command)
    if not status:
        log_msg = "%s : INFO  删除 %s  成功 \n"%(log_time,tmp_image)
    else:
        log_msg = "%s : ERROR  删除 %s  失败 : %s \n"%(log_time,tmp_image,str(out))
    fo.write(log_msg)
    fo.close()


def backup():
    for i in backup_images.keys():
        uuid = i
        name = backup_images[i]['name']
        local = backup_images[i]['local']
        current_datetime = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        tmp_image = tmp_dir + '/' + current_datetime+'_'+uuid[0:6]+'_'+name+'.img'
        opt_image_tgz = local + '/' + current_datetime+'_'+uuid[0:6]+'_'+name+'.img.tgz'

        print "DEBUG  tmp_image,opt_image_tgz  %s %s"%(tmp_image,opt_image_tgz)

        status,out=commands.getstatusoutput(gen_export_img_tmp(uuid=uuid,tmp_image=tmp_image))
        # export ceph success
        if not status:
            print "INFO ceph export done! %s"%(tmp_image)
            if compress_img(tmp_image=tmp_image,opt_image_tgz=opt_image_tgz) >= 0:
                print "INFO compressed image %s done"%(opt_image_tgz)
                rm_tmp_image(tmp_image)

            else:
                print "ERROR compressed image %s 请参考日志 源文件在%s,压缩失败文件在%s"%(tmp_image,opt_image_tgz)





# exec_commands()
backup()
