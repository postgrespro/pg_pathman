#!/usr/bin/env python

import subprocess
import getpass

DOCKER_ID = 'pathman'
pg_versions = ['9.5','9.6','10']

image_types = {
	'clang_check_code': {
		'CHECK_CODE': 'clang',
	},
	'cppcheck': {
		'CHECK_CODE': 'cppcheck',
	},
	'pathman_tests': {
		'CHECK_CODE': 'false',
	}
}

user = input("Enter username for `docker login`: ")
password = getpass.getpass()
subprocess.check_output([
	'docker',
	'login',
	'-u', user,
	'-p', password])

travis_conf_line = '- DOCKER_IMAGE=%s'
travis_conf = []
print("")

for pg_version in pg_versions:
	pgname = 'pg%s' % pg_version.replace('.', '')
	for key, variables in image_types.items():
		image_name = '%s/%s_%s' % (DOCKER_ID, pgname, key)
		with open('Dockerfile', 'w') as out:
			with open('Dockerfile.tmpl', 'r') as f:
				for line in f:
					line = line.replace('${PG_VERSION}', pg_version)
					for key, value in variables.items():
						varname = '${%s}' % key
						line = line.replace(varname, value)

					out.write(line)

		args = [
			'docker',
			'build',
			'-t', image_name,
			'.'
		]
		subprocess.check_output(args, stderr=subprocess.STDOUT)
		print("build ok:", image_name)
		subprocess.check_output(['docker', 'push', image_name],
				stderr=subprocess.STDOUT)
		print("upload ok:", image_name)
		travis_conf.append(travis_conf_line % image_name)

print("\ntravis configuration")
print('\n'.join(travis_conf))
