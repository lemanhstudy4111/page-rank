import math
from numbers import Number
import sys
import gzip

"""
API to retrieve doc name from id
"""


def get_reverse_name_id(linkids: dict[str, int]):
    return {linkid: link_name for link_name, linkid in linkids.items()}


"""
Helper function to create link id for each link => replace string comparison for faster computation
format: {name: id}
"""


def read_infile(input_file: str):
    with gzip.open(input_file, "rt", encoding="utf8") as infile:
        data = infile.read().strip().split("\n")
    return data


def create_linkids(input_data: str) -> dict[str, id]:
    linkids = {}
    cnt = 0
    for line in input_data:
        pair = line.split()
        src, target = pair[0], pair[1]
        if src not in linkids.keys():
            linkids[src] = cnt
            cnt += 1
        if target not in linkids.keys():
            linkids[target] = cnt
            cnt += 1
    return linkids


def create_outlinks(input_data: str, linkids: dict[str, id]) -> dict[int, int]:
    pairid = {id: 0 for _, id in linkids.items()}
    for line in input_data:
        pair = line.split()
        src_id = linkids[pair[0]]
        pairid[src_id] += 1
    return pairid


def create_inlinks(
    input_data: str, linkids: dict[str, id]
) -> dict[int, list[int]]:
    pairid = {id: [] for _, id in linkids.items()}
    for line in input_data:
        pair = line.split()
        src_id, target_id = linkids[pair[0]], linkids[pair[1]]
        pairid[target_id].append(src_id)
    return pairid


def get_no_outlinks(outlinks: dict[int, int]):
    res = []
    for outlink_id, num_outlinks in outlinks.items():
        if num_outlinks == 0:
            res.append(outlink_id)
    return res


def sum_inlinks(
    link: int,
    inlinks: dict[int, list[int]],
    outlinks: dict[int, int],
    prev_pr: dict[int, float],
    lamb: float,
    N: int,
    sum_no_outlinks: list[int],
):
    sum_frac = lamb / N + (1 - lamb) * (
        sum(
            [
                prev_pr[inlink_id] / outlinks[inlink_id]
                for inlink_id in inlinks[link]
            ],
        )
        + sum_no_outlinks
    )
    return sum_frac


def get_l2(prev_pr: dict[int, float], next_pr: dict[int, float]):
    return math.sqrt(
        sum((prev_pr[link] - next_pr[link]) ** 2 for link in prev_pr.keys())
    )


"""
Debugging functions and write functions
"""


# write dictionary for linkids(map[str, int]), pairid(map[int, int])
def write_dictionary(output_file, dictionary):
    with open(output_file, "wt", encoding="utf8") as outfile:
        for key, value in dictionary.items():
            outfile.write("{key}: {value}\n".format(key=key, value=value))
    return 0


def write_inlinks(
    output_file: str,
    k: int,
    inlinks: dict[int, list[int]],
    id_name: dict[int, str],
) -> int:
    sorted_inlinks = sorted(
        [(id_name[page], len(inlink)) for page, inlink in inlinks.items()],
        key=lambda x: (-x[1], x[0]),
    )
    with open(output_file, "w", encoding="utf8") as outfile:
        for i, (page_name, inlink) in enumerate(sorted_inlinks):
            if i >= k:
                break
            outfile.write(f"{page_name}\t{i + 1}\t{inlink}\n")
    return 0


def write_pr(
    output_file: str, k: int, pr: dict[int, float], id_name: dict[int, str]
) -> int:
    sorted_page_rank = sorted(
        [(id_name[page], rank) for page, rank in pr.items()],
        key=lambda x: (-x[1], x[0]),
    )
    with open(output_file, "w", encoding="utf8") as outfile:
        for i, (page_name, rank) in enumerate(sorted_page_rank):
            if i >= k:
                break
            outfile.write(f"{page_name}\t{i + 1}\t{rank:.12f}\n")

    return 0


"""
Main functions
"""


def do_pagerank_to_convergence(
    input_file: str,
    lamb: float,
    tau: Number,
    inlinks_file: str,
    pagerank_file: str,
    k: int,
):
    """Iterates the PageRank algorithm until convergence."""
    # TODO
    in_data = read_infile(input_file)
    linkids = create_linkids(in_data)
    idnames = get_reverse_name_id(linkids)
    outlinks = create_outlinks(in_data, linkids)
    no_outlinks = get_no_outlinks(outlinks)
    inlinks = create_inlinks(in_data, linkids)
    BIG_N = len(linkids)
    # Initializing page rank lamb/N
    prev_pr = {link_id: 1 / BIG_N for link_id in inlinks.keys()}
    next_pr = {}
    sum_l2 = 0
    while True:
        sum_no_outlink = (
            1
            / BIG_N
            * (sum(prev_pr[no_outlink_id] for no_outlink_id in no_outlinks))
        )
        for link_id in prev_pr.keys():
            curr_sum_inlinks = sum_inlinks(
                link_id,
                inlinks,
                outlinks,
                prev_pr,
                lamb,
                BIG_N,
                sum_no_outlink,
            )
            next_pr[link_id] = curr_sum_inlinks
        l2 = get_l2(prev_pr, next_pr)
        if l2 < tau:
            break
        prev_pr = next_pr.copy()
    write_inlinks(inlinks_file, k, inlinks, idnames)
    write_pr(pagerank_file, k, next_pr, idnames)
    return 0


def do_pagerank_n_times(
    input_file: str,
    lamb: float,
    N: int,
    inlinks_file: str,
    pagerank_file: str,
    k: int,
):
    """Iterates the PageRank algorithm N times."""
    # TODO
    in_data = read_infile(input_file)
    linkids = create_linkids(in_data)
    idnames = get_reverse_name_id(linkids)
    outlinks = create_outlinks(in_data, linkids)
    no_outlinks = get_no_outlinks(outlinks)
    inlinks = create_inlinks(in_data, linkids)
    BIG_N = len(linkids)
    # Initializing page rank lamb/N
    prev_pr = {link_id: 1 / BIG_N for link_id in inlinks.keys()}
    next_pr = {}
    for i in range(N):
        sum_no_outlink = (
            1
            / BIG_N
            * (sum(prev_pr[no_outlink_id] for no_outlink_id in no_outlinks))
        )
        for link_id in prev_pr.keys():
            next_pr[link_id] = sum_inlinks(
                link_id,
                inlinks,
                outlinks,
                prev_pr,
                lamb,
                BIG_N,
                sum_no_outlink,
            )
        prev_pr = next_pr.copy()
    write_inlinks(inlinks_file, k, inlinks, idnames)
    write_pr(pagerank_file, k, next_pr, idnames)
    return 0


def main():
    argc = len(sys.argv)
    input_file = (
        sys.argv[1] if argc > 1 else "links.srt.gz"
    )  # "C:\\Users\\lemin\\px-redo\\amherst.srt.gz"
    lamb = float(sys.argv[2]) if argc > 2 else 0.2

    tau = 0.005
    N = -1  # signals to run until convergence
    if argc > 3:
        arg = sys.argv[3]
        if arg.lower().startswith("exactly"):
            N = int(arg.split(" ")[1])
        else:
            tau = float(arg)

    inlinks_file = sys.argv[4] if argc > 4 else "inlinks.txt"
    pagerank_file = sys.argv[5] if argc > 5 else "pagerank.txt"
    k = int(sys.argv[6]) if argc > 6 else 100

    if N == -1:
        do_pagerank_to_convergence(
            input_file, lamb, tau, inlinks_file, pagerank_file, k
        )
    else:
        do_pagerank_n_times(
            input_file, lamb, N, inlinks_file, pagerank_file, k
        )

    # ...


if __name__ == "__main__":
    main()
