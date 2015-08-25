import luigi
import luigi.scheduler
import luigi.worker
import logging
from NetflixTask1 import PigJobTask1
from NetflixTask2 import PigJobTask2

#logger = logging.getLogger('luigi-interface')
 
class MyWorkflow2(luigi.Task):
    '''
    An example luigi workflow task, which runs a whole workflow of other luigi
    tasks when executed.
    '''
    # Here we define the whole workflow in the requires function
    def requires(self):
        print ':::::::::::::::::::::::::::::::::::::::::::::.'
        task_a = PigJobTask1()
        task_b = PigJobTask2(upstream_task=task_a)
        # task_b = PigJobTask2()
        return task_b
 
    # Define a simple marker file to show that the workflow has completed
    def output(self):
        return luigi.LocalTarget("/usr/local/hadoop/luigi-1.3.0/examples/myexamples/prueba" + '.workflow_complete')
 
    # Just write some text to the marker file
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Workflow complete!')
 

 
# If this file is executed as a script, let luigi take care of it:
if __name__ == '__main__':
    print ':::::::::::::::::::::::::::::::::::::::::::::.'
    luigi.run(["--local-scheduler"], main_task_cls=MyWorkflow2)
