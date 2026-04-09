from setuptools import setup

setup(
    name="pulsivo-salesman",
    version="0.1.0",
    description="Official Python client for the PulsivoSalesman Agent OS REST API",
    py_modules=["pulsivo_salesman_sdk", "pulsivo_salesman_client"],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
