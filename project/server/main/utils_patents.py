from itertools import combinations

NB_MAX_CO_ELEMENTS = 20


def get_co_occurences(my_list, my_field):
    elts_to_combine = [a for a in my_list if a.get(my_field)]
    values_to_combine = list(set([a[my_field] for a in elts_to_combine]))
    values_to_combine.sort()
    if len(values_to_combine) <= NB_MAX_CO_ELEMENTS:
        co_occurences = list(set(combinations(values_to_combine, 2)))
        co_occurences.sort()
        res = [f"{a}---{b}" for (a, b) in co_occurences]
        return res
    return None
