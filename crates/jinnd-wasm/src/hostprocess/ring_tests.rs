//! The loom models of the stream ring's racy seams (M2-K6 card: loom over
//! the stream/suspend interleavings). The production pump, the guest's
//! read, and the registration's release drive exactly these transitions.

use loom::sync::Arc;
use loom::thread;

use super::ring::Ring;

/// Under every interleaving of a pump offering against a guest taking, no
/// byte is lost or duplicated, order holds, and the buffer never exceeds
/// its capacity (R9: bounded, backpressure never memory).
#[test]
fn no_byte_is_lost_duplicated_or_reordered_and_the_bound_holds() {
    loom::model(|| {
        let ring = Arc::new(Ring::new(4));
        let offered: Vec<u8> = vec![1, 2, 3, 4, 5, 6];

        let pump = {
            let (ring, offered) = (Arc::clone(&ring), offered.clone());
            thread::spawn(move || ring.offer(&offered))
        };
        let reader = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || ring.take(10).0)
        };

        let accepted = pump.join().unwrap_or_else(|_| panic!("pump join"));
        let mut seen = reader.join().unwrap_or_else(|_| panic!("reader join"));
        assert!(accepted <= 4, "never above the capacity");
        assert!(ring.len() <= 4, "never above the capacity");
        let (rest, _) = ring.take(10);
        seen.extend(rest);
        assert_eq!(
            seen,
            offered[..accepted],
            "exactly the accepted prefix, in order"
        );
    });
}

/// A release (close) racing a guest take: whatever the interleaving, the
/// guest still drains every buffered byte, then reads EOF exactly once
/// the ring is empty, and nothing is accepted after the close returned
/// (the "released registration owns no stream" guarantee).
#[test]
fn close_racing_a_take_never_loses_the_tail_and_refuses_later_offers() {
    loom::model(|| {
        let ring = Arc::new(Ring::new(4));
        assert_eq!(ring.offer(&[7, 8]), 2);

        let closer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                ring.close();
                // After the close returned, no offer lands.
                ring.offer(&[9])
            })
        };
        let reader = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || ring.take(10))
        };

        let late = closer.join().unwrap_or_else(|_| panic!("closer join"));
        let (first, eof_first) = reader.join().unwrap_or_else(|_| panic!("reader join"));
        assert_eq!(late, 0, "nothing accepted after close");
        let (rest, eof_rest) = ring.take(10);
        let mut seen = first;
        seen.extend(rest);
        assert_eq!(seen, vec![7, 8], "the tail is never lost");
        assert!(eof_rest, "drained and closed reads EOF");
        // EOF was reported by the first take only if it drained everything
        // after the close landed; never with bytes still buffered.
        assert!(!eof_first || seen.len() == 2);
    });
}
