#!/usr/bin/env python3

import os
import subprocess
import getpass
import requests
import tempfile

from urllib.parse import urljoin
from urllib.request import urlopen

DOCKER_ID = 'pathman'
ALPINE_BASE_URL = 'https://raw.githubusercontent.com/docker-library/postgres/master/9.6/alpine/'
ALPINE_ENTRYPOINT = 'docker-entrypoint.sh'
ALPINE_PATCH = b'''
diff --git a/Dockerfile b/Dockerfile
index 9878023..ba215bc 100644
--- a/Dockerfile
+++ b/Dockerfile
@@ -80,6 +80,7 @@ RUN set -ex \\
 # configure options taken from:
 # https://anonscm.debian.org/cgit/pkg-postgresql/postgresql.git/tree/debian/rules?h=9.5
 	&& ./configure \\
+		--enable-cassert \\
 		--build="$gnuArch" \\
 # "/usr/src/postgresql/src/backend/access/common/tupconvert.c:105: undefined reference to `libintl_gettext'"
 #		--enable-nls \\
'''
CUSTOM_IMAGE_NAME = "%s/postgres_stable" % DOCKER_ID

def make_alpine_image(image_name):
	dockerfile = urlopen(urljoin(ALPINE_BASE_URL, 'Dockerfile')).read()
	entrypoint_sh = urlopen(urljoin(ALPINE_BASE_URL, ALPINE_ENTRYPOINT)).read()

	with tempfile.TemporaryDirectory() as tmpdir:
		print("Creating build in %s" % tmpdir)
		with open(os.path.join(tmpdir, 'Dockerfile'), 'w') as f:
			f.write(dockerfile.decode())

		with open(os.path.join(tmpdir, ALPINE_ENTRYPOINT), 'w') as f:
			f.write(entrypoint_sh.decode())

		with open(os.path.join(tmpdir, 'cassert.patch'), 'w') as f:
			f.write(ALPINE_PATCH.decode())

		subprocess.check_output(["git", "apply", "cassert.patch"], cwd=tmpdir)
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
	('pg96_ca', CUSTOM_IMAGE_NAME),
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
