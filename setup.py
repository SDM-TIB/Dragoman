import setuptools
import argparse
import time
import sys

v_time = str(int(time.time()))

parser = argparse.ArgumentParser()
parser.add_argument('-k', help="Release type", dest="kind")
#parser.add_argument('-t', help="Version tag", dest="tag")
parsed, rest = parser.parse_known_args()
sys.argv = [sys.argv[0]] + rest


with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as fh:
    v = fh.read().replace("\n", "")
    if parsed.kind == "rel":
        vers_taged = v
    else:
        vers_taged = v+".dev"+v_time


with open("requirements.txt") as r:
    requirements = list(filter(None, r.read().split("\n")[0:]))

setuptools.setup(
    name="dragoman_tool",
    version=vers_taged,
    author="Samaneh Jozashoori, Enrique Iglesias",
    author_email="samaneh.jozashoori@tib.eu, enrique.iglesias@tib.eu",
    license="Apache 2.0",
    description="An Optimized, RML-engine-agnostic Interpreter for Functional Mappings. It planns the optimized execution of FnO functions integrated in RML mapping rules, interprets and transforms the rules into function-free ones efficiently. Since Dragoman is engine-agnostic it can be adopted by any RML-compliant Knowledge Graph creation framework.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SDM-TIB/Dragoman",
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Software Development :: Pre-processors",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    install_requires=requirements,
    python_requires='>=3.6',
)
