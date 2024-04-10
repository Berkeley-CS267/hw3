#pragma once

#include "kmer_t.hpp"
#include <upcxx/upcxx.hpp>

struct HashMap {

    upcxx::dist_object<upcxx::global_ptr<kmer_pair>> g_data_ptr;
    upcxx::dist_object<upcxx::global_ptr<int>> g_used_ptr;
    upcxx::global_ptr<int> g_my_size_ptr;
    
    upcxx::promise<> insert_prom;

    size_t my_size;

    size_t size() const noexcept;
    // int size() const noexcept;

    HashMap(size_t size);

    // Most important functions: insert and retrieve
    // k-mers from the hash table.
    bool insert(const kmer_pair& kmer, upcxx::atomic_domain<int>* atomic_domain);
    bool find(const pkmer_t& key_kmer, kmer_pair& val_kmer, upcxx::atomic_domain<int>* atomic_domain);

    // Helper functions
    uint64_t get_target_rank(uint64_t kmer_hash);
    void wait_for_insert_completions();

    // Write and read to a logical data slot in the table.
    void write_slot(uint64_t slot, uint64_t target_rank, const kmer_pair& kmer);
    kmer_pair read_slot(uint64_t slot, uint64_t target_rank);

    // Request a slot or check if it's already used.
    bool request_slot(uint64_t slot, uint64_t target_rank, upcxx::atomic_domain<int>* atomic_domain);
    bool slot_used(uint64_t slot, uint64_t target_rank);
};

HashMap::HashMap(size_t size) {
    g_data_ptr = upcxx::new_array<kmer_pair>(size);
    g_used_ptr = upcxx::new_array<int>(size);
    g_my_size_ptr = upcxx::new_<int>(size);

    my_size = size;
}

uint64_t HashMap::get_target_rank(uint64_t kmer_hash) {
    return kmer_hash % upcxx::rank_n();
}

// Waits on all outgoing RMA writes. Need to call this before reading stage
void HashMap::wait_for_insert_completions() {
    upcxx::future<> f = insert_prom.finalize();
    f.wait();
}

bool HashMap::insert(const kmer_pair& kmer, upcxx::atomic_domain<int>* atomic_domain) {
    uint64_t hash = kmer.hash();
    uint64_t probe = 0;
    bool success = false;

    uint64_t target_rank = this->get_target_rank(hash);

    do {
        //Works assuming sizes across ranks are equal
        uint64_t slot = (hash + probe++) % size();
        success = request_slot(slot, target_rank, atomic_domain);
        if (success) {
            write_slot(slot, target_rank, kmer);
        }
    } while (!success && probe < size());
    return success;
}

bool HashMap::find(const pkmer_t& key_kmer, kmer_pair& val_kmer, upcxx::atomic_domain<int>* atomic_domain) {
    uint64_t hash = key_kmer.hash();
    uint64_t probe = 0;
    bool success = false;

    uint64_t target_rank = this->get_target_rank(hash);

    do {
        uint64_t slot = (hash + probe++) % size();
        if (slot_used(slot, target_rank)) {
            val_kmer = read_slot(slot, target_rank);
            if (val_kmer.kmer == key_kmer) {
                success = true;
            }
        }
    } while (!success && probe < size());
    return success;
}

// bool HashMap::slot_used(uint64_t slot) { return used[slot] != 0; }

// This is only checking and not modifying - use in HashMap::find only!
bool HashMap::slot_used(uint64_t slot, uint64_t target_rank) { 
    upcxx::global_ptr<int> target_used_ptr = g_used_ptr.fetch(target_rank).wait();
    if (target_used_ptr.is_local()) {
        // Downcast if this is local, saves times
        return target_used_ptr.local()[slot] != 0;
    }
    return upcxx::rget(target_used_ptr + slot).wait() != 0; 
}

// void HashMap::write_slot(uint64_t slot, const kmer_pair& kmer) { data[slot] = kmer; }

void HashMap::write_slot(uint64_t slot, uint64_t target_rank, const kmer_pair& kmer) { 
    upcxx::global_ptr<kmer_pair> target_data_ptr = g_data_ptr.fetch(target_rank).wait();
    if (target_data_ptr.is_local()) {
        target_data_ptr.local()[slot] = kmer;
    } else {
        // The wait completing objects are stored as promise. Need to sync this before reading
        upcxx::rput(&kmer, target_data_ptr + slot, std::memory_order_relaxed, upcxx::operation_cx::as_promise(insert_prom));
    }
}

// kmer_pair HashMap::read_slot(uint64_t slot) { return data[slot]; }

kmer_pair HashMap::read_slot(uint64_t slot, uint64_t target_rank) {
    upcxx::global_ptr<kmer_pair> target_data_ptr = g_data_ptr.fetch(target_rank).wait();
    if (target_data_ptr.is_local()) {
        return target_data_ptr.local()[slot];
    } else {
        return upcxx::rget(target_data_ptr).wait();
    }
}

// bool HashMap::request_slot(uint64_t slot) {
//     if (used[slot] != 0) {
//         return false;
//     } else {
//         used[slot] = 1;
//         return true;
//     }
// }

//Requests slot. TODO: To speed up, we can increment probe by value at used[slot]
bool HashMap::request_slot(uint64_t slot, uint64_t target_rank, upcxx::atomic_domain<int>* atomic_domain) {
    
    upcxx::global_ptr<int> target_used_ptr = g_used_ptr.fetch(target_rank).wait();
    uint64_t slot_val = atomic_domain->fetch_inc(target_used_ptr + slot, std::memory_order_acq_rel).wait();
    //TODO: should we downcast when available here? I think not, to maintain atomicity via ad
    
    if (slot_val > 0){
        //can increment probe here, if passed in
        return false;
    } else {
        return true;
    }
}

size_t HashMap::size() const noexcept { return my_size; }

// int HashMap::size() const noexcept { return g_my_size_ptr.local(); }

