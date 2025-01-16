from itertools import combinations

NB_MAX_CO_ELEMENTS = 20


def patents_remove_duplicates_ids(id_names):
    unique_ids = set()
    unique_id_names = set()
    for id_name in id_names:
        id = id_name.split("###")[0]
        if id not in unique_ids:
            unique_ids.add(id)
            unique_id_names.add(id_name)
    return list(unique_id_names)

def patents_get_co_occurences(my_list, my_field):
    elts_to_combine = [a for a in my_list if a.get(my_field)]
    values_to_combine = list(set([a[my_field] for a in elts_to_combine]))
    values_to_combine = patents_remove_duplicates_ids(values_to_combine)
    values_to_combine.sort()
    if len(values_to_combine) <= NB_MAX_CO_ELEMENTS:
        co_occurences = list(set(combinations(values_to_combine, 2)))
        co_occurences.sort()
        res = [f"{a}---{b}" for (a, b) in co_occurences]
        return res
    return None


def patents_applicants_add_idnames(applicants):
    for applicant in applicants:
        name = applicant.get("name")
        if not name:
            continue

        country = "FR" if applicant.get("country") == "FR" else "NOT_FR"

        id = name.lower()
        for current_id in applicant.get("ids", []):
            if current_id.get("type") == "siren":
                id = current_id.get("id")
                break

        id_name = f"{id}###{name}###{country}"
        applicant.update({"id_name": id_name})

    return applicants


def patents_cpc_add_idnames(cpcs):
    for cpc in cpcs:
        label = cpc.get("label")
        code = cpc.get("code")
        if not label or not code:
            continue

        id_name = f"{code}###{label}"
        cpc.update({"id_name": id_name})

    return cpcs
