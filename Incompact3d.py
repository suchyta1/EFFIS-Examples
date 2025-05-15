#!/usr/bin/env python3

import argparse
import shutil
import os
import f90nml

from effis.composition import Workflow, EffisLogger, Input
from effis.composition.runner import slurm, andes, perlmutter


class suchyta:

    andes = {
        'incompat3d' : "/ccs/home/esuchyta/software/install/andes/Incompact3d-gcc-9.3.0",
        'adios' : "/ccs/home/esuchyta/software/install/andes/adios2-gcc-9.3.0",
        'cpus' : 32,
    }

    perlmutter = {
        'incompat3d' : "/global/homes/e/esuchyta/software/install/perlmutter/Incompact3d-gcc",
        'adios' : "/global/homes/e/esuchyta/software/install/perlmutter/adios2-gcc",
        'cpus' : 128,
    }


def SetupArgs():

    runner = Workflow.DetectRunnerInfo()
    if (runner is not None) and (not isinstance(runner, slurm)):
        EffisLogger.RaiseError(ValueError, "Current example batch setup is for Slurm")

    slurmparser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Run Configuration
    ##############################################
    slurmparser.add_argument(
        "-o", "--outdir",
        type=str,
        help="Path to run directory",
        required=True,
    )

    if isinstance(runner, (andes, perlmutter)):
        slurmparser.add_argument(
            "-e", "--example",
            type=str,
            help="Which example to run",
            required=False,
            default=os.path.join(getattr(suchyta, runner.__class__.__name__)['incompat3d'], "examples", "TGV-Taylor-Green-Vortex", "input_DNS_Re1600.i3d"),
        )
        slurmparser.add_argument(
            "-s", "--suchyta",
            action="store_true",
            help="Use Eric Suchyta's software",
        )
    else:
        slurmparser.add_argument(
            "-e", "--example",
            type=str,
            help="Which example to run",
            required=True,
        )

    ##############################################

    # Scheduler Properties
    ##############################################
    slurmparser.add_argument(
        "-c", "--charge",
        type=str,
        help="Account to charge",
        required=True,
        dest="Charge",
    )
    slurmparser.add_argument(
        "-n", "--nodes",
        type=int,
        help="Number of nodes to use",
        required=False,
        default=1,
        dest="Nodes",
    )
    slurmparser.add_argument(
        "-w", "--walltime",
        type=str, 
        help="Wall time to request",
        required=False,
        default="00:15:00",
        dest="Walltime",
    )
    if isinstance(runner, perlmutter):
        slurmparser.add_argument(
            "-q", "--qos",
            type=str,
            help="QOS",
            required=False,
            default="regular",
            dest="QOS"
        )
        slurmparser.add_argument(
            "-k", "--constraint",
            type=str,
            help="cpu or gpu",
            required=False,
            default="cpu",
            dest="Constraint"
        )

    ##############################################

    # MPI properties
    ##############################################
    if isinstance(runner, (andes, perlmutter)):
        slurmparser.add_argument(
            "-r", "--RanksPerNode",
            required=False,
            help="Ranks per node to use",
            type=int,
            default=getattr(suchyta, runner.__class__.__name__)['cpus'],
        )
    else:
        slurmparser.add_argument(
            "-r", "--RanksPerNode",
            required=False,
            help="Ranks per node to use",
            type=int,
            default=32
        )
    ##############################################

    args = slurmparser.parse_args()
    return args, runner


def SetEnv(args, machine):

    env = {}

    if ('suchyta' in dir(args)) and args.suchyta:

        os.environ['PATH'] = "{0}:{1}".format(
            os.path.join(getattr(suchyta, machine)['incompat3d'], "bin"),
            os.environ['PATH']
        )

        env['LD_LIBRARY_PATH'] = "{0}:{1}".format(
            os.path.join(getattr(suchyta, machine)['adios'], "lib64"),
            os.environ['LD_LIBRARY_PATH']
        )

    return env


def ValidationRestartOff(Simulation):
    '''
    In the xcompat3d code,
    it says it's doing a crude checkpoint validation thing 
    that is is checking someting with directory sizes,
    which doesn't work with scratch space and ADIOS
    '''

    n = f90nml.Namelist()
    n["InOutParam"] = {}
    n["InOutParam"]["validation_restart"] = False

    filename = os.path.join(Simulation.Directory, "input.i3d")
    nml = f90nml.read(filename)
    nml["InOutParam"]["validation_restart"] = False

    tmp = "{0}.tmp".format(filename)
    shutil.copy(filename, tmp)
    os.remove(filename)
    f90nml.patch(tmp, n, out_path=filename)
    os.remove(tmp)

    lines = []
    with open(filename, "r" ) as infile:
        for line in infile.readlines():
            lines += [line.strip()]

    with open(filename, "w") as outfile:
        outfile.write("\n".join(lines) + "\n")



if __name__ == "__main__":

    args, runner = SetupArgs()

    extra = {}
    for key in ('Nodes', 'Walltime', 'Charge', 'QOS', 'Constraint'):
        extra[key] = getattr(args, key)

    MyWorkflow = Workflow(
        Runner=runner,
        Directory=args.outdir,
        **extra,
    )

    env = SetEnv(args, MyWorkflow.Runner.__class__.__name__)
 
    AppPath = shutil.which("xcompact3d")
    if AppPath is None:
        EffisLogger.RaiseError(FileNotFoundError, "xcompact3d not in $PATH")
    ExampleDir = os.path.dirname(args.example)

    InfoMPI = {
        'RanksPerNode': args.RanksPerNode,
        'Ranks': args.RanksPerNode * args.Nodes,
        'Nodes': args.Nodes
    }

    Simulation = MyWorkflow.Application(
        cmd=AppPath,
        Name="Simulation",
        Environment=env,
        **InfoMPI,
    )
    for filename in os.listdir(ExampleDir):
        Simulation.Input += os.path.join(ExampleDir, filename)
    Simulation.Input += Input(args.example, rename="input.i3d")

    MyWorkflow.Create()
    ValidationRestartOff(Simulation)
    MyWorkflow.Submit()

