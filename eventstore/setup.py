from setuptools import setup, find_packages

setup(name='eventstore',
      version='1.0',
      url='https://github.ibm.com/NGP-TWC/bluoltp',
      packages=find_packages(),
      zip_safe=False,
      install_requires=[
          'Sphinx', 
          'py4j', 
          'jupyter', 
          'matplotlib'
      ],
      include_package_data=True
)
