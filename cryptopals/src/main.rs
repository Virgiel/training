fn main() {
    println!("Hello, world!");
}

fn parse_hex(hex: &str, bytes: &mut Vec<u8>) {
    fn decode(b: u8) -> u8 {
        match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b + 10 - b'a',
            b'A'..=b'F' => b + 10 - b'A',
            b => unreachable!("not hex {b}"),
        }
    }
    for chunk in hex.as_bytes().chunks(2) {
        let first = chunk[0];
        let second = chunk.get(1).unwrap_or(&0);
        bytes.push(decode(first) * 16 + decode(*second))
    }
}

fn fmt_hex(bytes: &[u8], out: &mut String) {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    for b in bytes {
        out.push(HEX[(b / 16) as usize]);
        out.push(HEX[(b % 16) as usize]);
    }
}

fn xor(a: &mut [u8], b: &[u8]) {
    for (a, b) in a.iter_mut().zip(b) {
        *a ^= b;
    }
}

fn c_xor(bytes: &mut [u8], c: u8) {
    for b in bytes {
        *b ^= c;
    }
}

fn fmt_base64(bytes: &[u8], base64: &mut String) {
    const BASE64: [char; 64] = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1',
        '2', '3', '4', '5', '6', '7', '8', '9', '+', '/',
    ];
    for chunk in bytes.chunks(3) {
        let mut bytes = [0; 4];
        bytes[1..chunk.len() + 1].copy_from_slice(chunk);
        let word = u32::from_be_bytes(bytes);
        let out_chunk: [u8; 4] = [
            (word >> 18) as u8,
            (word << 14 >> 26) as u8,
            (word << 20 >> 26) as u8,
            (word << 26 >> 26) as u8,
        ];
        base64.push(BASE64[out_chunk[0] as usize]);
        base64.push(BASE64[out_chunk[1] as usize]);
        base64.push(BASE64[out_chunk[2] as usize]);
        base64.push(BASE64[out_chunk[3] as usize]);
    }
}

#[cfg(test)]
mod tests {
    use crate::{c_xor, fmt_base64, fmt_hex, parse_hex, xor};

    #[test]
    fn s1c1() {
        let from = "49276d206b696c6c696e6720796f757220627261696e206c696b65206120706f69736f6e6f7573206d757368726f6f6d";
        let to = "SSdtIGtpbGxpbmcgeW91ciBicmFpbiBsaWtlIGEgcG9pc29ub3VzIG11c2hyb29t";
        let mut buf = Vec::new();
        parse_hex(&from, &mut buf);
        let mut result = String::new();
        fmt_base64(&buf, &mut result);
        assert_eq!(result, to);
    }

    #[test]
    fn s1c2() {
        let a = "1c0111001f010100061a024b53535009181c";
        let b = "686974207468652062756c6c277320657965";
        let c = "746865206b696420646f6e277420706c6179";
        let mut b_a = Vec::new();
        let mut b_b = Vec::new();
        parse_hex(&a, &mut b_a);
        parse_hex(&b, &mut b_b);
        xor(&mut b_a, &b_b);
        let mut result = String::new();
        fmt_hex(&b_a, &mut result);
        assert_eq!(result, c);
    }

    #[test]
    fn s1c3() {
        let cypher = "1b37373331363f78151b7f2b783431333d78397828372d363c78373e783a393b3736";
        let key = "X";
        let mut buf = Vec::new();
        parse_hex(cypher, &mut buf);
        for c in b'A'..=b'Z' {
            c_xor(&mut buf, c);
            if let Ok(str) = std::str::from_utf8(&buf) {
                println!("{c} {}", str);
            }
            c_xor(&mut buf, c);
        }
        panic!("lol");
    }
}
