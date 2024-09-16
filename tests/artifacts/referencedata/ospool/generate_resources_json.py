import xml.etree.ElementTree as ET
import time
from urllib.request import urlopen
from urllib.error import HTTPError
import json


TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"


def get_topology_resource_data() -> dict:
    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries:
                raise
        else:
            break
    resource_groups = xmltree.getroot()

    resources_map = []
    resources_spec_map = []
    for resource_group in resource_groups:
        resources = resource_group.find("Resources")
        for resource in resources:

            # Services with ID 1 are CEs
            found_ce = False
            for service in resource.find("Services"):
                id_elem = service.find("ID")
                if int(id_elem.text.strip()) == 1:
                    found_ce = True
                    break
            if not found_ce:
                continue

            resource_map = {}
            resource_name = resource.find("Name").text
            resource_map["name"] = resource_name
            resource_map["resource"] = resource_name
            resource_map["description"] = resource.find("Description").text
            resource_map["resource_type"] = "HTC"
            resources_map.append(resource_map)

            resource_spec_map = {}
            resource_spec_map["resource"] = resource_name
            resource_spec_map["nodes"] = 0
            resource_spec_map["processors"] = 0
            resource_spec_map["ppn"] = 0
            resources_spec_map.append(resource_spec_map)

    return resources_map, resources_spec_map


def main():
    resources_map, resources_spec_map = get_topology_resource_data()
    json.dump(resources_map, open("resources.json", "w"), indent=4)
    json.dump(resources_spec_map, open("resource_specs.json", "w"), indent=4)


if __name__ == "__main__":
    main()
