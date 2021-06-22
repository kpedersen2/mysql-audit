/*
 * audit_log_buffer.h
 *
 *  Created on: Jun 17, 2021
 *      Author: kevinp
 */

#ifndef AUDIT_LOG_BUFFER_H_
#define AUDIT_LOG_BUFFER_H_

#include "mysql_inc.h"

#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>

class LogBuffer {
private:
	using value_type = char;
	using container_type = std::vector< value_type >;
	using size_type = container_type::size_type;
	using iterator = container_type::iterator;
	using const_iterator = container_type::const_iterator;

public:
	LogBuffer(size_type size) {
		m_buffer.reserve( size );
	}

	bool chk_buffer(size_type size) const {
		return (m_buffer.size() + size) < m_buffer.capacity();
	}

	iterator insert(const char* data, size_type size) {
		return m_buffer.insert( m_buffer.end(), data, data + size );
	}

	size_type last_write_pos() {
		return std::distance( m_buffer.begin(), m_last_write_pos );
	}

	size_type size() const noexcept {
		return m_buffer.size();
	}

	size_type max_size() const noexcept {
		return m_buffer.max_size();
	}

	iterator begin() noexcept {
		return m_buffer.begin();
	}

	iterator end() noexcept {
		return m_buffer.end();
	}

	const_iterator cbegin() const noexcept {
		return m_buffer.cbegin();
	}

	const_iterator cend() const noexcept {
		return m_buffer.cend();
	}

	void clear() noexcept {
		m_buffer.clear();
	}

	size_type write_size() const noexcept {
		return m_buffer.size();
	}

	uchar* write_start() noexcept {
		return reinterpret_cast<uchar*>(m_buffer.data());
	}

private:
	container_type m_buffer;

	iterator m_last_write_pos = m_buffer.begin();
};

class LogManager {
public:
	~LogManager();

	void set_file(FILE *file);
	
    ssize_t write(const char* data, size_t size);

    ssize_t write_to_disk();

    void fsync_monitor();

    bool is_full_durability_mode() const;

	void set_full_durability_mode(bool mode);

	void start_fsync_thread();

	void stop_fsync_thread();

private:
    FILE *m_log_file;

	LogBuffer m_buffer1{ 10000 };
	LogBuffer m_buffer2{ 10000 };

	LogBuffer* m_write_buffer = &m_buffer1;
	LogBuffer* m_read_buffer = &m_buffer2;
	LogBuffer* m_last_buffer_wrote_to = &m_buffer1;

	std::thread m_fsync_thread;

	std::mutex m_buffer_mutex;
	std::condition_variable m_writer_signal;
	std::condition_variable m_fsync_signal;

	std::atomic_bool m_stop_thread{ false };
	std::atomic_bool m_fsync_success{ false };
	bool buffer_ready = false;

	std::chrono::time_point<std::chrono::steady_clock> m_next_group_fsync = std::chrono::steady_clock::now();
	std::chrono::duration<uint, std::milli> m_group_fsync_period{};

    bool m_full_durability_mode = false;
};

#endif //AUDIT_LOG_BUFFER_H_
