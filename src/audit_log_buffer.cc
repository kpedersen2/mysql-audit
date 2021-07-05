#include "audit_log_buffer.h"

LogManager::LogManager(LogBuffer::size_type buffer_size) :
	m_buffer1{ buffer_size },
	m_buffer2{ buffer_size }
{
}

LogManager::~LogManager()
{
	stop_fsync_thread();
}

void LogManager::set_file(FILE *file)
{
	m_log_file = file;
}

ssize_t LogManager::write(const char* data, size_t size)
{
	ssize_t res = -1;
	if(m_log_file)
	{
		sql_print_information("audit plugin: writing %zu bytes to buffer thread.", size);

		while (true) {
			std::unique_lock lck{m_buffer_mutex};

			if (m_incoming_buffer->chk_buffer(size)) {
				sql_print_information("audit plugin: can add to buffer");
				m_incoming_buffer->insert(data, size);
				sql_print_information("audit plugin: outgoing buffer size: %lu, incoming buffer size: %lu", m_outgoing_buffer->size(), m_incoming_buffer->size());
			}
			else {
				sql_print_information("audit plugin: buffer is full, signal writer thread and wait until empty");

				buffer_ready = false;
				// Signal the writer thread that there is a client waiting to write to the buffer.
				m_writer_signal.notify_one();

				// Now wait until the log buffer is ready to write to.
				m_writer_signal.wait(lck, [this]{ return buffer_ready; } );

				continue;
			}

			if (is_full_durability_mode()) {
				// In full durability mode we wait until the log message was
				// successfully written to disk before proceeding.
				m_last_buffer_wrote_to = m_incoming_buffer;

				sql_print_information("audit plugin: waiting until fsync is successful");
				m_fsync_signal.wait(lck); // TODO: Timeout here?
				if (m_fsync_success) {
					m_fsync_success = false;
					sql_print_information("audit plugin: fsync succeeded");
				}
				else {
					sql_print_error("audit plugin: fsync failed");
				}
			}
			res = 1;
			break;
		}
	}
	else {
		sql_print_error("Audit log file is not open");
	}
	return res;
}

ssize_t LogManager::write_to_disk()
{
	ssize_t res = my_fwrite(m_log_file, m_outgoing_buffer->write_start(), m_outgoing_buffer->size(), MYF(0));
	if ( res ) {
		res = (fflush(m_log_file) == 0);
		if (res)
		{
			int fd = fileno(m_log_file);
			res = (my_sync(fd, MYF(MY_WME)) == 0);
		}
	}
	if ( res < 0 ) {
		sql_print_error("failed writing to file. Err: %s", strerror(errno));
	}

	return res;
}

void LogManager::fsync_monitor()
{
	using namespace std::chrono_literals;

	sql_print_information("fsync monitor: starting");

	while (true) {
		if (m_stop_thread) {
			sql_print_information("fsync monitor: shutting down");
			m_stop_thread = false;
			break;
		}

		std::unique_lock lck{m_buffer_mutex};
		if (is_full_durability_mode()) {
			// Wait until the next fsync timeout
			if (std::cv_status::timeout == m_writer_signal.wait_until(lck, m_next_group_fsync)) {
				//sql_print_information("fsync monitor: write thread woken up on timeout");
			}
			else {
				sql_print_information("fsync monitor: write thread woken up by client thread, buffer must be full");
			}
		}
		else {
			// Wait until time out or signal
			auto ret = m_writer_signal.wait_for(lck, 2s);
			if ( ret == std::cv_status::no_timeout) {
				sql_print_information("fsync monitor: writer thread signalled that buffer has data");
			}
			else {
				//sql_print_information("fsync monitor: writer signal timed out");
			}
		}

		if (m_incoming_buffer->size() > 0)
		{
			sql_print_information("fsync monitor: switching buffer pointers");
			// When waking there is something to write. Switch the pointers then unlock.
			if (m_outgoing_buffer == &m_buffer1) {
				m_outgoing_buffer = &m_buffer2;
				m_incoming_buffer = &m_buffer1;
			}
			else {
				m_outgoing_buffer = &m_buffer1;
				m_incoming_buffer = &m_buffer2;
			}
			buffer_ready = true;
			lck.unlock();

			sql_print_information("fsync monitor: signal waiting threads that buffers are ready");
			m_writer_signal.notify_all();

			auto ret = write_to_disk();
			if (ret > 0) {
				m_fsync_success = true;

				// Success update pointers
				sql_print_information("fsync monitor: successful write_to_disk");
				if (is_full_durability_mode()) {
					m_next_group_fsync = std::chrono::steady_clock::now() + m_group_fsync_period;
	
					sql_print_information("fsync monitor: signal waiting threads that fsync was successful, wrote %d log messages", m_outgoing_buffer->num_messages());
					m_fsync_signal.notify_all();
				}
				m_outgoing_buffer->clear();
				sql_print_information("fsync monitor: outgoing buffer size: %lu, incoming buffer size: %lu", m_outgoing_buffer->size(), m_incoming_buffer->size());
			}
			else {
				// Error, try again?				
			}
		}
	}
}

LogBuffer::size_type LogManager::log_buffer_capacity() const
{
	return m_buffer1.capacity();
}

bool LogManager::is_full_durability_mode() const
{
    return m_full_durability_mode;
}

void LogManager::set_full_durability_mode(bool mode) {
	if (mode && m_full_durability_mode != mode) {
		// Initialize fsync timeout
		m_next_group_fsync = std::chrono::steady_clock::now();
	}

	m_full_durability_mode = mode;
}

void LogManager::start_fsync_thread()
{
	m_fsync_thread = std::thread{ &LogManager::fsync_monitor, this };
}

void LogManager::stop_fsync_thread()
{
	m_stop_thread = true;
	if (m_fsync_thread.joinable()) {
		m_fsync_thread.join();
	}
}

void LogManager::set_buffer_size(LogBuffer::size_type size)
{
	m_buffer1.reserve(size);
	m_buffer2.reserve(size);
}
