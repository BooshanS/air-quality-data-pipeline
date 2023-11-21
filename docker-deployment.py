from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from flows.etl_deployment import parent_etl


docker_block = DockerContainer.load("air-quality-docker")

docker_dep= Deployment.build_from_flow(flow=parent_etl, name='docker-flow', infrastructure=docker_block)

if __name__=="__main__":
    docker_dep.apply()
