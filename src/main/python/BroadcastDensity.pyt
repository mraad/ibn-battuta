import os
import subprocess
import threading
import Queue
import re
import glob
import json

import sys
import arcpy



#
# http://stefaanlippens.net/python-asynchronous-subprocess-pipe-reading
#
class AsyncReader(threading.Thread):
    def __init__(self, fd, queue):
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        for line in iter(self._fd.readline, ''):
            self._queue.put(line.rstrip())

    def eof(self):
        return not self.is_alive() and self._queue.empty()


class Toolbox(object):
    def __init__(self):
        self.label = "Broadcast Density Toolbox"
        self.alias = "Broadcast Density Toolbox"
        self.description = "Toolbox with tools to submit BroadcastDensity Spark job, and to view the density result"
        self.tools = [SubmitJobTool, ViewTool]


class SubmitTool(object):
    def __init__(self):
        self.canRunInBackground = False

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def submit(self, properties_file, arr):

        spark_home = os.environ.get('SPARK_HOME')
        if spark_home is None:
            arcpy.AddError('Please define system environment variable SPARK_HOME !')
            return

        args = [os.path.join(spark_home, 'bin', 'spark-submit.cmd'),
                "--conf", "spark.ui.enabled=false",
                "--conf", "spark.master.rest.enabled=false",
                # "--conf", "spark.local.dir=" + arcpy.env.scratchFolder,
                "--properties-file", properties_file,
                "--packages", "org.geotools:gt-ogr-bridj:${gt.version},org.geotools:gt-cql:${gt.version}",
                "--jars", "${artifactId}-${version}.jar",
                "broadcast-density.py",
                ] + arr

        create_no_window = 0x08000000
        proc = subprocess.Popen(args,
                                cwd=sys.path[0],
                                bufsize=4096,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                creationflags=create_no_window)

        stdout_queue = Queue.Queue()
        stdout_reader = AsyncReader(proc.stdout, stdout_queue)
        stdout_reader.start()

        stderr_queue = Queue.Queue()
        stderr_reader = AsyncReader(proc.stderr, stderr_queue)
        stderr_reader.start()

        yy = re.compile('^\d\d')
        while not stdout_reader.eof() or not stderr_reader.eof():
            while not stdout_queue.empty():
                line = stdout_queue.get()
                if yy.match(line):
                    arcpy.AddMessage(line)
            while not stderr_queue.empty():
                line = stderr_queue.get()
                if yy.match(line):
                    arcpy.AddMessage(line)

        stdout_reader.join()
        stderr_reader.join()

        proc.stdout.close()
        proc.stderr.close()


class SubmitJobTool(SubmitTool):
    def __init__(self):
        super(SubmitJobTool, self).__init__()
        self.label = "Submit Job"
        self.description = "Tool to submit Broadcast Density Spark job"

    def getParameterInfo(self):
        prop_file = arcpy.Parameter(
            name="prop_file",
            displayName="Properties File",
            direction="Input",
            datatype="File",
            parameterType="Required")
        prop_file.value = "broadcast-density.conf"

        input_path = arcpy.Parameter(
            name="input_path",
            displayName="Input Path",
            direction="Input",
            datatype="Geodataset",
            parameterType="Required")
        input_path.value = "C:\\Temp\\Miami.gdb\\Broadcast"

        output_path = arcpy.Parameter(
            name="output_path",
            displayName="Output Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        output_path.value = "C:\\temp\\output"

        cell_size = arcpy.Parameter(
            name="cell_size",
            displayName="Cell Size",
            direction="Input",
            datatype="Double",
            parameterType="Required")
        cell_size.value = 0.001

        filter_count = arcpy.Parameter(
            name="filter_count",
            displayName="Filter Count",
            direction="Input",
            datatype="Long",
            parameterType="Required")
        filter_count.value = 10

        return [prop_file, input_path, output_path, cell_size, filter_count]

    def execute(self, parameters, messages):
        self.submit(parameters[0].valueAsText, [
            parameters[1].valueAsText,
            parameters[2].valueAsText,
            parameters[3].valueAsText,
            parameters[4].valueAsText
        ])


class ViewTool(object):
    def __init__(self):
        self.label = "View Results"
        self.description = "Tool to view the local result of the Broadcast Density Job"
        self.canRunInBackground = False

    def getParameterInfo(self):
        output_fc = arcpy.Parameter(
            name="output_fc",
            displayName="output_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        input_path = arcpy.Parameter(
            name="input_path",
            displayName="Input Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        input_path.value = "C:\\temp\\output\\part-*"

        fc_name = arcpy.Parameter(
            name="fc_name",
            displayName="Layer Name",
            direction="Input",
            datatype="String",
            parameterType="Required")
        fc_name.value = "BroadcastDensity"

        return [output_fc, input_path, fc_name]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        sp_ref = arcpy.SpatialReference(4326)

        ws = "in_memory"
        name = parameters[2].value
        fc = ws + "/" + name
        # fc = os.path.join(arcpy.env.scratchGDB, name)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arcpy.management.CreateFeatureclass(ws, name, "POINT",
                                            spatial_reference=sp_ref,
                                            has_m="DISABLED",
                                            has_z="DISABLED")
        arcpy.management.AddField(fc, "POPULATION", "LONG")
        with arcpy.da.InsertCursor(fc, ['SHAPE@XY', 'POPULATION']) as cursor:
            part_files = glob.glob(parameters[1].value)
            for part_file in part_files:
                with open(part_file, "r") as f:
                    for line in f:
                        doc = json.loads(line.rstrip())
                        lon = doc['lon']
                        lat = doc['lat']
                        pop = doc['pop']
                        cursor.insertRow(((lon, lat), pop))
        parameters[0].value = fc
        return
