#!/usr/bin/env python

import subprocess

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

stopline = '###STOP'

password = input("Enter password for `docker login`: ")
subprocess.check_output([
	'docker',
	'login',
	'-u', DOCKER_ID,
	'-p', password])

for pg_version in pg_versions:
	pgname = 'pg%s' % pg_version.replace('.', '')
	for key, variables in image_types.items():
		image_name = '%s/%s_%s' % (DOCKER_ID, pgname, key)
		with open('Dockerfile', 'w') as out:
			with open('Dockerfile.tmpl', 'r') as f:
				for line in f:
					if line.startswith(stopline):
						break

					line = line
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
		exit()
