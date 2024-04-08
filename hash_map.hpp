#pragma once

#include "kmer_t.hpp"
#include <upcxx/upcxx.hpp>

struct HashMap {

    upcxx::global_ptr<kmer_pair> g_data_ptr;
    upcxx::global_ptr<int> g_used_ptr;
    upcxx::global_ptr<int> g_my_size_ptr;

    std::vector<kmer_pair> data;
    std::vector<int> used;

    size_t my_size;

    size_t size() const noexcept;
    // int size() const noexcept;

    HashMap(size_t size);

    // Most important functions: insert and retrieve
    // k-mers from the hash table.
    bool insert(const kmer_pair& kmer);
    bool find(const pkmer_t& key_kmer, kmer_pair& val_kmer);

    // Helper functions
    uint64_t get_target_rank(const kmer_pair& kmer);

    // Write and read to a logical data slot in the table.
    void write_slot(uint64_t slot, const kmer_pair& kmer);
    kmer_pair read_slot(uint64_t slot);

    // Request a slot or check if it's already used.
    bool request_slot(uint64_t slot);
    bool slot_used(uint64_t slot);
};

HashMap::HashMap(size_t size) {
    g_data_ptr = upcxx::new_array<kmer_pair>(size);
    g_used_ptr = upcxx::new_array<int>(size);
    g_my_size_ptr = upcxx::new_<int>(size);

    my_size = size;
    data.resize(size);
    used.resize(size, 0);
}

bool HashMap::insert(const kmer_pair& kmer) {
    uint64_t hash = kmer.hash();
    uint64_t probe = 0;
    bool success = false;
    do {
        uint64_t slot = (hash + probe++) % size();
        success = request_slot(slot);
        if (success) {
            write_slot(slot, kmer);
        }
    } while (!success && probe < size());
    return success;
}

bool HashMap::find(const pkmer_t& key_kmer, kmer_pair& val_kmer) {
    uint64_t hash = key_kmer.hash();
    uint64_t probe = 0;
    bool success = false;
    do {
        uint64_t slot = (hash + probe++) % size();
        if (slot_used(slot)) {
            val_kmer = read_slot(slot);
            if (val_kmer.kmer == key_kmer) {
                success = true;
            }
        }
    } while (!success && probe < size());
    return success;
}

// bool HashMap::slot_used(uint64_t slot) { return used[slot] != 0; }

bool HashMap::slot_used(uint64_t slot) { return g_used_ptr.local()[slot] != 0; }

// void HashMap::write_slot(uint64_t slot, const kmer_pair& kmer) { data[slot] = kmer; }

void HashMap::write_slot(uint64_t slot, const kmer_pair& kmer) { g_data_ptr.local()[slot] = kmer; }

// kmer_pair HashMap::read_slot(uint64_t slot) { return data[slot]; }

kmer_pair HashMap::read_slot(uint64_t slot) { return g_data_ptr.local()[slot]; }

// bool HashMap::request_slot(uint64_t slot) {
//     if (used[slot] != 0) {
//         return false;
//     } else {
//         used[slot] = 1;
//         return true;
//     }
// }

bool HashMap::request_slot(uint64_t slot) {
    if (g_used_ptr.local()[slot] != 0) {
        return false;
    } else {
        g_used_ptr.local()[slot] = 1;
        return true;
    }
}

size_t HashMap::size() const noexcept { return my_size; }

// int HashMap::size() const noexcept { return g_my_size_ptr.local(); }

uint64_t get_target_rank(const kmer_pair& kmer) {
    return kmer.hash() % upcxx::rank_n();
}
