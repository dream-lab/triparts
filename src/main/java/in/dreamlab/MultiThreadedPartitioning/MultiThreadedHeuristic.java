package in.dreamlab.MultiThreadedPartitioning;

public enum MultiThreadedHeuristic {
    /* a. NormDynamic--             Only Bloom Filter(BF) entries are checked else search for current lightest partition
     * b  TrNormDynamic--           Only TMAP + BF entries are checked else search for current lightest partition
     * c. HDNormDynamic --          Only HDMap + BF entries are checked else search for current lightest partition
     * d. TrHDNormDynamic--         All TMAP + HDMAP + BF entries are checked
     * e. TrHDNormEdgeDuplDynamic-- TrHDNormDynamic + EdgeDuplication in certain scenarios
     * */
    B, BT, BH, BTH, BTHE;
}
