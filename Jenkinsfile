// ── fd-proxy-service — build image on the cheap cluster, push to Harbor ───────
//
// Same shape as the platform repo's Jenkinsfile (fd-craw-private): the built-in
// agent is the Jenkins pod on cheap-3 with the host docker CLI +
// /var/run/docker.sock mounted, so `docker build` uses cheap-3's daemon — the
// one whose daemon.json already lists 100.64.0.8:30880 as an insecure registry.
//
// Verified on that host (probe, 2026-09-15):
//   * registry-1.docker.io is UNREACHABLE — plain `FROM python:3.12-slim` cannot
//     be pulled. Hence BASE_IMAGE, passed as a build-arg; the Dockerfile keeps
//     the plain name as its default so GitHub Actions is unaffected.
//   * docker.m.daocloud.io and docker.1panel.live both serve
//     library/python:3.12-slim, and inside that image apt-get update and pip
//     install (PyPI directly, Tsinghua as backup) both succeed.
//
// Deployment is pull-based: pushing a tag plus a pod restart IS the deploy.
//
// Requires Jenkins credential `harbor-fd-push` (Username with password) — a
// robot with push on the `finddata` project of 100.64.0.8:30880.

pipeline {
    agent any

    options {
        timestamps()
        // Cold builds are the norm (nothing is cached between runs) and the
        // pip install is the slow part; a false timeout throws the work away.
        timeout(time: 60, unit: 'MINUTES')
        // Two builds would fight over the same :latest tag and over cheap-3's disk.
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    parameters {
        string(name: 'TAG', defaultValue: '', description: 'Image tag (default: sha-<short sha>)')
    }

    environment {
        REGISTRY   = '100.64.0.8:30880'
        IMAGE      = '100.64.0.8:30880/finddata/fd-proxy-service'
        // cheap-3 cannot reach Docker Hub. Fallback if daocloud ever breaks:
        // docker.1panel.live (also verified), or mirror the base into Harbor's
        // `library` project once a library-scoped push credential exists.
        BASE_IMAGE = 'docker.m.daocloud.io/library/python:3.12-slim'
    }

    stages {
        stage('Show Commit') {
            steps {
                script {
                    def sha = sh(returnStdout: true, script: 'git rev-parse --short=7 HEAD').trim()
                    def msg = sh(returnStdout: true, script: 'git log -1 --pretty=%s').trim()
                    env.TAG = params.TAG ?: "sha-${sha}"
                    echo "COMMIT: ${sha} — ${msg}"
                    echo "Building ${IMAGE}:${env.TAG} from ${BASE_IMAGE}"
                }
            }
        }

        stage('Build') {
            steps {
                sh 'docker build --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$IMAGE:$TAG" -t "$IMAGE:latest" .'
            }
        }

        // A build that compiles is not an image that runs. The default CMD is
        // `fd-proxy-service --help`, which exercises the venv, the console
        // script and every module import without needing redis or the DB.
        stage('Smoke test') {
            steps {
                sh 'docker run --rm "$IMAGE:$TAG" > /dev/null && echo SMOKE_OK'
            }
        }

        stage('Push to Harbor') {
            steps {
                withCredentials([usernamePassword(
                        credentialsId: 'harbor-fd-push',
                        usernameVariable: 'HARBOR_USER',
                        passwordVariable: 'HARBOR_PASS')]) {
                    sh '''
                        set -e
                        echo "$HARBOR_PASS" | docker login "$REGISTRY" -u "$HARBOR_USER" --password-stdin
                        docker push "$IMAGE:$TAG"
                        docker push "$IMAGE:latest"
                    '''
                }
            }
        }
    }

    post {
        always {
            sh 'docker logout "$REGISTRY" || true'
        }
        success {
            echo "Pushed ${IMAGE}:${TAG} — point k8s/proxy-*.yaml at this tag"
        }
    }
}
