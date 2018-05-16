rel1 is arranged

rel1.filter_map(f1)
    .arrange_by_key()
    .join_core(rel2.filter_map(f2).arrange_by_key(), f12)
