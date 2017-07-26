#!/usr/bin/env python3

import os
import subprocess
import getpass
import requests
import tempfile

from urllib.parse import urljoin
from urllib.request import urlopen

DOCKER_ID = 'pathman'
ALPINE_BASE_URL = 'https://raw.githubusercontent.com/docker-library/postgres/master/10/alpine/'
ALPINE_ENTRYPOINT = 'docker-entrypoint.sh'
ALPINE_PATCH = b'''
--- Dockerfile	2017-07-25 12:43:20.424984422 +0300
+++ Dockerfile	2017-07-25 12:46:10.279267520 +0300
@@ -86,6 +86,7 @@
 		--enable-integer-datetimes \\
 		--enable-thread-safety \\
 		--enable-tap-tests \\
+		--enable-cassert \\
 # skip debugging info -- we want tiny size instead
 #		--enable-debug \\
 		--disable-rpath \\

'''
CUSTOM_IMAGE_NAME = "%s/postgres_stable" % DOCKER_ID

def make_alpine_image(image_name):
	dockerfile = urlopen(urljoin(ALPINE_BASE_URL, 'Dockerfile')).read()
	entrypoint_sh = urlopen(urljoin(ALPINE_BASE_URL, ALPINE_ENTRYPOINT)).read()

	with tempfile.TemporaryDirectory() as tmpdir:
		print("Creating build in %s" % tmpdir)
		patch_name = os.path.join(tmpdir, "cassert.patch")

		with open(os.path.join(tmpdir, 'Dockerfile'), 'w') as f:
			f.write(dockerfile.decode())

		with open(os.path.join(tmpdir, ALPINE_ENTRYPOINT), 'w') as f:
			f.write(entrypoint_sh.decode())

		with open(patch_name, 'w') as f:
			f.write(ALPINE_PATCH.decode())

		with open(patch_name, 'r') as f:
			p = subprocess.Popen(["patch", "-p0"], cwd=tmpdir, stdin=subprocess.PIPE)
			p.communicate(str.encode(f.read()))
		print("patch applied")
		subprocess.check_output(["docker", "build", ".", '-t', image_name], cwd=tmpdir)
		print("build ok: ", image_name)
		subprocess.check_output(['docker', 'push', image_name],
				stderr=subprocess.STDOUT)
		print("upload ok:", image_name)

make_alpine_image(CUSTOM_IMAGE_NAME)

pg_containers = [
	('pg95', 'postgres:9.5-alpine'),
	('pg96', 'postgres:9.6-alpine'),
	('pg10', 'postgres:10-alpine'),
	('pg10_ca', CUSTOM_IMAGE_NAME),
]

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

if __name__ == '__main__':
	for pgname, container in pg_containers:
		for key, variables in image_types.items():
			image_name = '%s/%s_%s' % (DOCKER_ID, pgname, key)
			with open('Dockerfile', 'w') as out:
				with open('Dockerfile.tmpl', 'r') as f:
					for line in f:
						line = line.replace('${PG_IMAGE}', container)
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
