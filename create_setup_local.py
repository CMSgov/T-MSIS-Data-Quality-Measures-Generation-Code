import re

# read in setup.py as text
with open('setup.py', 'r') as f:
    text = f.read()

# update the version to use the .env variable
text_out = re.sub('version="\d{1,2}\.\d{1,2}\.\d{1,2}"', "version=os.environ['VERSION']", text, flags=re.I)
text_out2 = text_out.replace("import setuptools\n", 
                             'import setuptools\nimport os\nif \'VERSION\' not in os.environ:\n    print("Please set VERSION environment variable, see workflow.md.")\n    quit(1)\n\n')

# write out new script
with open('setup_local.py', 'w') as f:
    f.write(text_out2)
