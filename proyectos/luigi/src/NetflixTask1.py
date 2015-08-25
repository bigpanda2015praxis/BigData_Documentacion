from luigi.hadoop import HadoopRunContext
import tempfile
import os
import luigi
import logging
import subprocess
import signal
import shlex
import select
from luigi import six
#from luigi.hdfs import HdfsTarget
from luigi import configuration
__author__ = 'victor'

logger = logging.getLogger('luigi-interface')

class PigJobTask1(luigi.Task):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("/usr/local/hadoop/luigi-1.3.0/examples/myexamples/Task1.complete")

    def pig_env_vars(self):
        return {}

    def pig_home(self):
        return configuration.get_config().get('pig', 'home', '/usr/local/hadoop/pig/')

    def pig_command_path(self):
        return luigi.configuration.get_config().get("pig", "command", "pig")

    def pig_script_path(self):
        return '/usr/local/hadoop/luigi-1.3.0/examples/myexamples/NetflixQuery1.pig'
        #return 'hdfs://user/hduser/netflix/pruebaNF.pig'

    def pig_options(self):
        return ['-x', 'local']
        #return []

    def pig_parameters(self):
        return {}

    def pig_properties(self):
        return {}

    def _build_pig_cmd(self):
        opts = self.pig_options()

        for k, v in six.iteritems(self.pig_parameters()):
            opts.append("-p")
            opts.append("%s=%s" % (k, v))

        if self.pig_properties():
            with open('pig_property_file', 'w') as prop_file:
                prop_file.writelines(["%s=%s%s" % (k, v, os.linesep) for (k, v) in six.iteritems(self.pig_properties())])
            opts.append('-propertyFile')
            opts.append('pig_property_file')

        cmd = [self.pig_command_path()] + opts + ["-f", self.pig_script_path()]

        return cmd

    def run(self):
        cmd = self._build_pig_cmd()
        self.track_and_progress(cmd)
        with self.output().open('w') as outfile:
            outfile.write('Task1 complete!')

    def track_and_progress(self, cmd):
        temp_stdout = tempfile.TemporaryFile()
        env = os.environ.copy()
        env['PIG_HOME'] = self.pig_home()
        for k, v in six.iteritems(self.pig_env_vars()):
            env[k] = v

        proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        
        reads = [proc.stderr.fileno(), proc.stdout.fileno()]
        # tracking the possible problems with this job
        err_lines = []
        with PigRunContext():
            while proc.poll() is None:
                ret = select.select(reads, [], [])
                for fd in ret[0]:
                    if fd == proc.stderr.fileno():
                        line = proc.stderr.readline().decode('utf8')
                        err_lines.append(line)
                    if fd == proc.stdout.fileno():
                        line = proc.stdout.readline().decode('utf8')
                        temp_stdout.write(line)

                err_line = line.lower()
                if err_line.find('More information at:') != -1:
                    logger.info(err_line.split('more information at: ')[-1].strip())
                if err_line.find(' - '):
                    t = err_line.split(' - ')[-1].strip()
                    if t != "":
                        logger.info(t)

        # Read the rest + stdout
        err = ''.join(err_lines + [err_line.decode('utf8') for err_line in proc.stderr])
        if proc.returncode == 0:
            logger.info("Job completed successfully!")
        else:
            logger.error("Error when running script:\n%s", self.pig_script_path())
            logger.error(err)
            raise PigJobError("Pig script failed with return value: %s" % (proc.returncode,), err=err)

class PigRunContext(object):
    def __init__(self):
        self.job_id = None

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def kill_job(self, captured_signal=None, stack_frame=None):
        if self.job_id:
            logger.info('Job interrupted, killing job %s', self.job_id)
            subprocess.call(['pig', '-e', '"kill %s"' % self.job_id])
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)


class PigJobError(RuntimeError):
    def __init__(self, message, out=None, err=None):
        super(PigJobError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err

    def __str__(self):
        info = self.message
        if self.out:
            info += "\nSTDOUT: " + str(self.out)
        if self.err:
            info += "\nSTDERR: " + str(self.err)
        return info

# if __name__ == '__main__':
# 	luigi.run(["--local-scheduler"], main_task_cls=PigJobTask1)
