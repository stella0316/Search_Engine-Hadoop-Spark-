import subprocess
to_read = input("enter file name: ")
print(to_read)
process = subprocess.check_output(["bash","cal_p.sh",to_read],stdout = subprocess.PIPE)
print(process.stdout)
#process.stdout.readline()


